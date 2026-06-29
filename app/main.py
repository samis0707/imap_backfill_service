"""
IMAP → JSON microservice for n8n RAG ingestion.

Replaces the imapflow Code node. Exposes two read endpoints:

  GET  /health
  POST /fetch          -> paginated bulk backfill (offset/limit) OR incremental (since_uid)
  GET  /mailboxes      -> list available folders

Auth: static bearer token via env IMAP_SERVICE_TOKEN.
IMAP creds via env: IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASS.

Design notes:
- Connection is opened per request and closed in a finally block (no shared
  long-lived connection -> avoids stale-socket issues behind Coolify/Traefik).
- ENVELOPE-only listing first (cheap), then bodies fetched for the page slice
  only. Keeps memory bounded regardless of mailbox size.
- Attachment text extraction (PDF/DOCX) is best-effort and never fatal: a
  corrupt PDF degrades to an empty attachment_text, the email still indexes.
"""

from __future__ import annotations

import io
import os
import logging
from typing import Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from imapclient import IMAPClient
import mailparser

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("imap-service")

# --- config ----------------------------------------------------------------
IMAP_HOST = os.environ.get("IMAP_HOST", "")
IMAP_PORT = int(os.environ.get("IMAP_PORT", "993"))
IMAP_USER = os.environ.get("IMAP_USER", "")
IMAP_PASS = os.environ.get("IMAP_PASS", "")
SERVICE_TOKEN = os.environ.get("IMAP_SERVICE_TOKEN", "")
MAX_BODY_CHARS = int(os.environ.get("MAX_BODY_CHARS", "8000"))
MAX_ATTACH_CHARS = int(os.environ.get("MAX_ATTACH_CHARS", "12000"))
MAX_ATTACH_BYTES = int(os.environ.get("MAX_ATTACH_BYTES", str(20 * 1024 * 1024)))

app = FastAPI(title="IMAP RAG service", version="1.0.0")


# --- models -----------------------------------------------------------------
class FetchRequest(BaseModel):
    mailbox: str = Field(default="INBOX")
    # bulk backfill
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=500)
    # incremental: only messages with UID strictly greater than this
    since_uid: Optional[int] = Field(default=None, ge=0)
    # include extracted attachment text
    with_attachments: bool = Field(default=True)


class EmailItem(BaseModel):
    uid: int
    message_id: str
    subject: str
    from_: str = Field(alias="from")
    to: str
    cc: str
    date: Optional[str]
    body: str
    attachment_text: str
    attachment_names: list[str]

    class Config:
        populate_by_name = True


class FetchResponse(BaseModel):
    mailbox: str
    total: int  # total matching messages in mailbox (for pagination math)
    returned: int
    next_offset: Optional[int]  # null when no more pages (offset mode)
    max_uid: Optional[int]  # highest UID in this page (for incremental cursor)
    items: list[EmailItem]


# --- auth -------------------------------------------------------------------
def _check_auth(authorization: Optional[str]) -> None:
    if not SERVICE_TOKEN:
        # If no token configured, refuse rather than run open.
        raise HTTPException(status_code=500, detail="IMAP_SERVICE_TOKEN not configured")
    expected = f"Bearer {SERVICE_TOKEN}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


# --- attachment extraction --------------------------------------------------
def _extract_pdf(data: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(data))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(parts).strip()
    except Exception as e:
        log.warning("PDF extract failed: %s", e)
        return ""


def _extract_docx(data: bytes) -> str:
    try:
        import docx
        d = docx.Document(io.BytesIO(data))
        return "\n".join(p.text for p in d.paragraphs).strip()
    except Exception as e:
        log.warning("DOCX extract failed: %s", e)
        return ""


def _attachment_text(parsed) -> tuple[str, list[str]]:
    chunks: list[str] = []
    names: list[str] = []
    for att in getattr(parsed, "attachments", []) or []:
        filename = att.get("filename") or ""
        payload = att.get("payload")
        binary = att.get("binary", False)
        if not payload:
            continue
        try:
            raw = payload if isinstance(payload, (bytes, bytearray)) else payload.encode("latin-1")
            if not binary:
                # mailparser sometimes returns base64 string payloads
                import base64
                try:
                    raw = base64.b64decode(payload)
                except Exception:
                    pass
        except Exception:
            continue
        if len(raw) > MAX_ATTACH_BYTES:
            names.append(f"{filename} (skipped: too large)")
            continue
        low = filename.lower()
        text = ""
        if low.endswith(".pdf"):
            text = _extract_pdf(raw)
        elif low.endswith(".docx"):
            text = _extract_docx(raw)
        else:
            continue  # only PDF/DOCX per requirement
        if text:
            names.append(filename)
            chunks.append(f"--- {filename} ---\n{text}")
    combined = "\n\n".join(chunks).strip()[:MAX_ATTACH_CHARS]
    return combined, names


# --- helpers ----------------------------------------------------------------
def _addr_list(parsed_field) -> str:
    # mailparser returns list of [name, address] pairs
    out = []
    for entry in parsed_field or []:
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            out.append(entry[1] or entry[0])
        elif isinstance(entry, str):
            out.append(entry)
    return ", ".join(a for a in out if a)


def _body_text(parsed) -> str:
    body = (parsed.text_plain[0] if parsed.text_plain else "").strip()
    if not body and parsed.text_html:
        import re
        html = parsed.text_html[0]
        body = re.sub(r"<[^>]+>", " ", html)
        body = re.sub(r"\s+", " ", body).strip()
    return body[:MAX_BODY_CHARS]


def _resolve_ipv4(host: str, port: int) -> str:
    import socket
    infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not infos:
        raise HTTPException(status_code=502, detail=f"No IPv4 address for {host}")
    return infos[0][4][0]


def _connect() -> IMAPClient:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        raise HTTPException(status_code=500, detail="IMAP credentials not configured")

    import socket
    # Docker bridge networks usually have no IPv6 egress. imaplib may pick the
    # AAAA record -> OSError 101 "Network is unreachable". We force IPv4 only
    # for the duration of this connect by filtering getaddrinfo to AF_INET,
    # while keeping IMAP_HOST as the connect target so TLS/SNI still validates
    # against the real certificate hostname.
    _orig_getaddrinfo = socket.getaddrinfo

    def _ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
        return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    socket.getaddrinfo = _ipv4_only
    try:
        client = IMAPClient(IMAP_HOST, port=IMAP_PORT, use_uid=True, ssl=True)
        client.login(IMAP_USER, IMAP_PASS)
    finally:
        socket.getaddrinfo = _orig_getaddrinfo
    return client


# --- endpoints --------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/mailboxes")
def mailboxes(authorization: Optional[str] = Header(default=None)):
    _check_auth(authorization)
    client = _connect()
    try:
        folders = [f[2] for f in client.list_folders()]
        return {"mailboxes": folders}
    finally:
        try:
            client.logout()
        except Exception:
            pass


@app.post("/fetch", response_model=FetchResponse)
def fetch(req: FetchRequest, authorization: Optional[str] = Header(default=None)):
    _check_auth(authorization)
    client = _connect()
    try:
        client.select_folder(req.mailbox, readonly=True)

        # Determine the candidate UID set.
        if req.since_uid is not None:
            # incremental: UID range search, strictly greater handled by slicing
            uids = client.search([u"UID", f"{req.since_uid + 1}:*"])
            # IMAP "n:*" can echo the last UID even if none are greater; filter.
            uids = [u for u in uids if u > req.since_uid]
        else:
            uids = client.search(["ALL"])

        uids = sorted(uids)
        total = len(uids)

        if req.since_uid is not None:
            page_uids = uids[: req.limit]
            next_offset = None
        else:
            page_uids = uids[req.offset : req.offset + req.limit]
            consumed = req.offset + len(page_uids)
            next_offset = consumed if consumed < total else None

        items: list[EmailItem] = []
        max_uid: Optional[int] = None

        if page_uids:
            resp = client.fetch(page_uids, ["RFC822"])
            for uid in page_uids:
                raw = resp.get(uid, {}).get(b"RFC822")
                if not raw:
                    continue
                try:
                    parsed = mailparser.parse_from_bytes(raw)
                except Exception as e:
                    log.warning("parse failed uid=%s: %s", uid, e)
                    continue

                att_text, att_names = ("", [])
                if req.with_attachments:
                    att_text, att_names = _attachment_text(parsed)

                msg_id = (parsed.message_id or "").strip() or str(uid)
                date_iso = parsed.date.isoformat() if parsed.date else None

                items.append(
                    EmailItem(
                        uid=uid,
                        message_id=msg_id,
                        subject=(parsed.subject or "").strip(),
                        **{"from": _addr_list(parsed.from_)},
                        to=_addr_list(parsed.to),
                        cc=_addr_list(parsed.cc),
                        date=date_iso,
                        body=_body_text(parsed),
                        attachment_text=att_text,
                        attachment_names=att_names,
                    )
                )
                max_uid = uid if max_uid is None else max(max_uid, uid)

        return FetchResponse(
            mailbox=req.mailbox,
            total=total,
            returned=len(items),
            next_offset=next_offset,
            max_uid=max_uid,
            items=items,
        )
    finally:
        try:
            client.logout()
        except Exception:
            pass
