import os
import logging
from textwrap import shorten

from dotenv import load_dotenv
from openai import OpenAI

from storage.sqlite_db import save_document_text, DocumentText

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


def translate_text(text: str, source_lang: str = "FR", target_lang: str = "EN") -> str:
    """
    Traduit le texte en utilisant un modèle OpenAI.
    Si aucune clé n'est configurée, renvoie le texte original.
    """
    if not client:
        logging.warning("OPENAI_API_KEY not set, returning original text as 'translation'")
        return text

    prompt = (
        f"Translate the following {source_lang} regulatory text into {target_lang}.\n"
        "Preserve technical and regulatory terms as much as possible.\n\n"
        f"Text:\n{text[:8000]}"
    )

    logging.info("Calling OpenAI API for translation...")
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a financial regulatory translation assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    translated = resp.choices[0].message.content.strip()
    return translated


def create_translation(text_row: DocumentText,
                       source_lang: str = "FR",
                       target_lang: str = "EN") -> DocumentText:
    """
    Traduit un DocumentText existant et sauvegarde la version traduite.
    """
    logging.info(
        "Translating document_text id=%s (doc_id=%s) from %s to %s",
        text_row.id, text_row.document_id, source_lang, target_lang
    )

    translated_text = translate_text(text_row.full_text, source_lang, target_lang)
    summary = shorten(translated_text, width=400, placeholder="...")

    translated_row = save_document_text(
        document_id=text_row.document_id,
        language=target_lang,
        is_original=False,
        text=translated_text,
        summary=summary,
    )

    return translated_row
