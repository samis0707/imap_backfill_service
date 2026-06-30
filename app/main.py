"""
IMAP → JSON microservice for n8n RAG ingestion.

Replaces the imapflow Code node. Exposes read endpoints:

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
- Cross-folder backfill: pass mailbox="*" (or omit it). The service lists all
  folders, sorts them deterministically, and resolves a GLOBAL offset across
  the concatenated (folder, uid) sequence. This keeps n8n pagination at a
  single offset cursor. message_id stays the dedup key (globally unique),
  so re-runs and overlap across folders dedup correctly in Qdrant.
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

# Folders to skip when iterating "*". Comma-separated, matched case-insensitive
# against the folder name. Default: skip Trash/Spam/Junk/Drafts. Keep Sent.
SKIP_FOLDERS = [
    s.strip().lower()
    for s in os.environ.get("SKIP_FOLDERS", "trash,papierkorb,spam,junk,drafts,entwürfe").split(",")
    if s.strip()
]

app = FastAPI(title="IMAP RAG service", version="1.1.0")


# --- models -----------------------------------------------------------------
class FetchRequest(BaseModel):
    mailbox: str = Field(default="INBOX")
    offset: int = Field(default=0, ge=0)          # lokaler Offset INNERHALB des aktuellen Ordners
    folder_index: int = Field(default=0, ge=0)    # welcher Ordner (Index in der sortierten Liste)
    limit: int = Field(default=50, ge=1, le=500)
    since_uid: Optional[int] = Field(default=None, ge=0)
    with_attachments: bool = Field(default=True)


class EmailItem(BaseModel):
    uid: int
    folder: str
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
    folder: Optional[str]            # welcher Ordner gerade verarbeitet wurde
    folder_index: int
    folder_total: int                # Mails im aktuellen Ordner
    returned: int
    next_offset: Optional[int]       # nächster lokaler Offset, oder null
    next_folder_index: Optional[int] # nächster Ordner-Index, oder null wenn fertig
    done: bool                       # true = alle Ordner durch
    max_uid: Optional[int]
    items: list[EmailItem]


# --- auth -------------------------------------------------------------------
def _check_auth(authorization: Optional[str]) -> None:
    if not SERVICE_TOKEN:
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
            continue
        if text:
            names.append(filename)
            chunks.append(f"--- {filename} ---\n{text}")
    combined = "\n\n".join(chunks).strip()[:MAX_ATTACH_CHARS]
    return combined, names


# --- helpers ----------------------------------------------------------------
def _addr_list(parsed_field) -> str:
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


def _connect() -> IMAPClient:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        raise HTTPException(status_code=500, detail="IMAP credentials not configured")

    import socket
    # Docker bridge networks usually have no IPv6 egress. Force IPv4 only for
    # the duration of connect while keeping IMAP_HOST as the TLS/SNI target.
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


def _list_backfill_folders(client) -> list[str]:
    """All selectable folders, deterministically sorted, skip-list applied."""
    folders = []
    for flags, delimiter, name in client.list_folders():
        # \Noselect folders cannot be opened (container nodes) -> skip
        flag_names = {
            (f.decode().lower() if isinstance(f, bytes) else str(f).lower())
            for f in (flags or [])
        }
        if "\\noselect" in flag_names:
            continue
        if name.lower() in SKIP_FOLDERS:
            continue
        folders.append(name)
    # deterministic order so a global offset stays stable across pages
    folders.sort()
    return folders


def _parse_one(uid: int, folder: str, raw: bytes, with_attachments: bool) -> Optional[EmailItem]:
    try:
        parsed = mailparser.parse_from_bytes(raw)
    except Exception as e:
        log.warning("parse failed folder=%s uid=%s: %s", folder, uid, e)
        return None

    att_text, att_names = ("", [])
    if with_attachments:
        att_text, att_names = _attachment_text(parsed)

    msg_id = (parsed.message_id or "").strip() or f"{folder}:{uid}"
    date_iso = parsed.date.isoformat() if parsed.date else None

    return EmailItem(
        uid=uid,
        folder=folder,
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

    all_folders_mode = (req.mailbox or "").strip() in ("", "*")

    # incremental is single-folder only
    if req.since_uid is not None and all_folders_mode:
        raise HTTPException(
            status_code=400,
            detail="since_uid (incremental) requires a single mailbox; '*' not allowed",
        )

    client = _connect()
    try:
        # ---- single-folder incremental (unchanged behaviour) --------------
        if req.since_uid is not None:
            client.select_folder(req.mailbox, readonly=True)
            uids = client.search([u"UID", f"{req.since_uid + 1}:*"])
            uids = sorted(u for u in uids if u > req.since_uid)
            total = len(uids)
            page_uids = uids[: req.limit]
            items: list[EmailItem] = []
            max_uid: Optional[int] = None
            if page_uids:
                resp = client.fetch(page_uids, ["RFC822"])
                for uid in page_uids:
                    raw = resp.get(uid, {}).get(b"RFC822")
                    if not raw:
                        continue
                    it = _parse_one(uid, req.mailbox, raw, req.with_attachments)
                    if it:
                        items.append(it)
                    max_uid = uid if max_uid is None else max(max_uid, uid)
            return FetchResponse(
                mailbox=req.mailbox, total=total, returned=len(items),
                next_offset=None, max_uid=max_uid, items=items,
            )

# ---- offset backfill, EIN Ordner pro Aufruf -----------------------
        if all_folders_mode:
            folders = _list_backfill_folders(client)
        else:
            folders = [req.mailbox]

        # Sind wir über das Ende aller Ordner hinaus? -> fertig
        if req.folder_index >= len(folders):
            return FetchResponse(
                mailbox=("*" if all_folders_mode else req.mailbox),
                folder=None, folder_index=req.folder_index,
                folder_total=0, returned=0,
                next_offset=None, next_folder_index=None,
                done=True, max_uid=None, items=[],
            )

        folder = folders[req.folder_index]
        client.select_folder(folder, readonly=True)
        uids = sorted(client.search(["ALL"]))   # nur EIN SEARCH, nur dieser Ordner
        folder_total = len(uids)

        page_uids = uids[req.offset : req.offset + req.limit]

        items: list[EmailItem] = []
        max_uid: Optional[int] = None
        if page_uids:
            resp = client.fetch(page_uids, ["RFC822"])
            for uid in page_uids:
                raw = resp.get(uid, {}).get(b"RFC822")
                if not raw:
                    continue
                it = _parse_one(uid, folder, raw, req.with_attachments)
                if it:
                    items.append(it)
                max_uid = uid if max_uid is None else max(max_uid, uid)

        # Cursor weiterschalten:
        consumed_in_folder = req.offset + len(page_uids)
        if consumed_in_folder < folder_total:
            # gleicher Ordner, nächste Seite
            next_offset = consumed_in_folder
            next_folder_index = req.folder_index
            done = False
        else:
            # Ordner fertig -> nächster Ordner ab Offset 0
            next_offset = 0
            next_folder_index = req.folder_index + 1
            done = next_folder_index >= len(folders)
            if done:
                next_offset = None
                next_folder_index = None

        return FetchResponse(
            mailbox=("*" if all_folders_mode else req.mailbox),
            folder=folder,
            folder_index=req.folder_index,
            folder_total=folder_total,
            returned=len(items),
            next_offset=next_offset,
            next_folder_index=next_folder_index,
            done=done,
            max_uid=max_uid,
            items=items,
        )
    finally:
        try:
            client.logout()
        except Exception:
            pass
