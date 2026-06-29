# IMAP RAG Service

Ersetzt den `imapflow`-Code-Node in n8n. Liest Mails per IMAP, extrahiert
Body (Plaintext, HTML→Text Fallback) und Attachment-Text (PDF/DOCX), gibt
saubere JSON-Batches zurück. Paginiert für Bulk-Backfill **und** inkrementell.

## Endpunkte

| Methode | Pfad | Zweck |
|---|---|---|
| GET | `/health` | Healthcheck (kein Auth) |
| GET | `/mailboxes` | Ordnerliste (Auth) |
| POST | `/fetch` | Mails holen (Auth) |

Auth: Header `Authorization: Bearer <IMAP_SERVICE_TOKEN>`.

### POST /fetch — Request

```jsonc
{
  "mailbox": "INBOX",
  "offset": 0,          // Bulk-Backfill: Seite ab Index
  "limit": 50,          // max 500
  "since_uid": null,    // gesetzt = inkrementell, ignoriert offset
  "with_attachments": true
}
```

### Response

```jsonc
{
  "mailbox": "INBOX",
  "total": 4123,        // Gesamtzahl Treffer (für Paginierungs-Mathematik)
  "returned": 50,
  "next_offset": 50,    // null = letzte Seite (Offset-Modus)
  "max_uid": 1187,      // höchste UID dieser Seite (Cursor für inkrementell)
  "items": [
    {
      "uid": 1187,
      "message_id": "<...>",
      "subject": "...",
      "from": "absender@x.de",
      "to": "...",
      "cc": "",
      "date": "2026-05-01T10:22:00+02:00",
      "body": "...",
      "attachment_text": "--- rechnung.pdf ---\n...",
      "attachment_names": ["rechnung.pdf"]
    }
  ]
}
```

## Zwei Betriebsarten

**Bulk-Backfill (einmalig):** `since_uid` weglassen, mit `offset=0` starten,
dann `next_offset` aus der Antwort als nächstes `offset` verwenden, bis
`next_offset = null`.

**Inkrementell (laufend):** höchste verarbeitete UID merken, künftig
`since_uid` = dieser Wert. Service liefert nur Mails mit größerer UID.
`max_uid` der Antwort wird zum neuen Cursor.

## Deploy in Coolify

1. Repo zu Coolify hinzufügen, Build Pack = **Docker Compose**.
2. Environment Variables setzen:
   - `IMAP_HOST`, `IMAP_PORT` (993), `IMAP_USER`, `IMAP_PASS`
   - `IMAP_SERVICE_TOKEN` (langer Zufallsstring — `openssl rand -hex 32`)
3. Deployen. Der Service hört intern auf Port 8080.
4. n8n erreicht ihn über den **Coolify-internen Docker-Netzwerknamen**
   (wie bei Qdrant/Presidio), z. B. `http://imap-service-xxxxxxxx:8080`.
   Kein öffentliches Routing nötig — der Service muss nicht von außen
   erreichbar sein.

> IMAP-Passwort: wenn der Provider es anbietet, ein **App-Passwort** statt
> des Hauptpassworts verwenden (jederzeit widerrufbar).

## n8n-Anbindung

Ersetze den `imapflow`-Code-Node durch einen **HTTP Request**-Node:

- Methode: `POST`
- URL: `http://<imap-service-intern>:8080/fetch`
- Authentication: Header Auth / Generic → `Authorization: Bearer <token>`
  (oder als n8n-Credential anlegen, wie deine Qdrant-Bearer-Auth)
- Body (JSON):
  ```json
  { "mailbox": "INBOX", "offset": 0, "limit": 50, "with_attachments": true }
  ```

Danach im Flow: `Split Out` auf `items[]`, sodass jede Mail ein n8n-Item
wird. Die Felder (`from`, `subject`, `date`, `message_id`, `body`,
`attachment_text`) mappst du direkt in deine bestehende Dedup-/Markdown-/
Qdrant-Kette. Für den Markdown-Build z. B. `body` + `attachment_text`
zusammenführen.

### Paginierungs-Loop in n8n

Für den vollständigen Backfill ohne Memory-Druck:
HTTP Request (offset=0) → verarbeite `items` → wenn `next_offset != null`,
erneut mit `offset = next_offset`. Realisierbar über einen `If` +
`SplitInBatches`-Rücklauf oder einen Sub-Workflow, dem du `offset` übergibst.
