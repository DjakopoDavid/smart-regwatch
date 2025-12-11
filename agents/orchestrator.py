import logging

from agents.extraction_agent import run_extraction, AUTHORITY_CONFIG
from agents.content_extraction_agent import process_document_for_text
from agents.translation_agent import create_translation
from agents.keyword_agent import analyse_document_text
from agents.notification_agent import send_email_notification

from storage.sqlite_db import (
    init_db, upsert_document, DocumentText, SessionLocal, Document
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def run_full_pipeline_for_authority(authority_code: str,
                                    original_language: str = "FR",
                                    target_language: str = "EN"):
    """
    Pipeline complet pour une autorité donnée :
    1. Extraction web (PDF/Excel/CSV/ZIP)
    2. Upsert dans SQLite
    3. Extraction du contenu (PDF, Excel, CSV)
    4. Traduction (original_language -> target_language)
    5. Analyse de mots-clés (original + traduction)
    6. Notification email si mots-clés détectés
    """
    if authority_code not in AUTHORITY_CONFIG:
        raise ValueError(f"Unknown authority: {authority_code}")

    logging.info("=== Initialising database ===")
    init_db()

    logging.info("=== Running extraction agent for %s ===", authority_code)
    metadata_list = run_extraction(authority_code, download_dir="data/raw")

    docs = []
    for meta in metadata_list:
        doc = upsert_document(meta)
        docs.append(doc)

    session = SessionLocal()
    try:
        for doc in docs:
            document = session.query(Document).get(doc.id)
            logging.info(
                "[%s] Processing document: id=%s, title=%s, filename=%s",
                authority_code, document.id, document.title, document.filename
            )

            # 1. Extraction de contenu
            process_document_for_text(document, language=original_language)

            # 2. Textes originaux
            original_texts = (
                session.query(DocumentText)
                .filter(
                    DocumentText.document_id == document.id,
                    DocumentText.is_original == True,
                )
                .order_by(DocumentText.created_at.desc())
                .all()
            )

            for ot in original_texts:
                # 3. Traduction
                translated = create_translation(
                    ot,
                    source_lang=original_language,
                    target_lang=target_language,
                )

                # 4. Analyse mots-clés sur original + traductions
                analyse_document_text(ot)
                analyse_document_text(translated)

            # 5. Notification email
            send_email_notification(document_id=document.id, min_keywords=1)

    finally:
        session.close()
