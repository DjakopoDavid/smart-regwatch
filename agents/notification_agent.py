import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

from storage.sqlite_db import log_notification, Document, SessionLocal, KeywordMatch, KeywordConfig

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_FROM = os.getenv("SMTP_FROM")
SMTP_TO_DEFAULT = os.getenv("SMTP_TO_DEFAULT", "")


def build_notification_body(document: Document, matches: list[KeywordMatch]) -> str:
    lines = []
    lines.append(f"Document: {document.title}")
    lines.append(f"Source: {document.source}")
    lines.append(f"URL: {document.url}")
    lines.append(f"Version: {document.version}")
    lines.append("")
    lines.append("Keywords detected:")

    for m in matches:
        kw: KeywordConfig = m.keyword
        lines.append(f"- {kw.keyword} (occurrences: {m.occurrences})")
        if m.context_snippet:
            lines.append(f"  Context: {m.context_snippet}")
            lines.append("")

    return "\n".join(lines)


def send_email_notification(document_id: int, min_keywords: int = 1) -> None:
    """
    Envoie un email si au moins 'min_keywords' mots-clés ont été détectés
    pour ce document.
    """
    recipients = [addr.strip() for addr in SMTP_TO_DEFAULT.split(",") if addr.strip()]
    if not (SMTP_HOST and SMTP_USER and SMTP_PASSWORD and SMTP_FROM and recipients):
        logging.warning("SMTP configuration incomplete, skipping email notification.")
        return

    session = SessionLocal()
    try:
        doc = session.query(Document).filter(Document.id == document_id).first()
        if not doc:
            logging.warning("Document not found for notification (id=%s)", document_id)
            return

        matches = (
            session.query(KeywordMatch)
            .filter(KeywordMatch.document_id == document_id)
            .all()
        )

        if len(matches) < min_keywords:
            logging.info(
                "Not enough keyword matches (%s) for document id=%s, skipping email.",
                len(matches), document_id
            )
            return

        subject = f"[RegWatch] New/Updated document: {doc.title}"
        body = build_notification_body(doc, matches)

        msg = MIMEMultipart()
        msg["From"] = SMTP_FROM
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_FROM, recipients, msg.as_string())
            status = "SENT"
            error_message = None
            logging.info("Notification email sent for document id=%s", document_id)
        except Exception as e:
            status = "ERROR"
            error_message = str(e)
            logging.error("Error sending email: %s", e)

        log_notification(
            document_id=document_id,
            subject=subject,
            recipients=",".join(recipients),
            status=status,
            error_message=error_message,
        )
    finally:
        session.close()
