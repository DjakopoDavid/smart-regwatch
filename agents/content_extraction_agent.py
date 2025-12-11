import logging
from pathlib import Path

from PyPDF2 import PdfReader
import pandas as pd

from storage.sqlite_db import save_document_text, Document

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def extract_text_from_pdf(file_path: str) -> str:
    text = []
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    reader = PdfReader(str(path))
    for page in reader.pages:
        page_text = page.extract_text() or ""
        text.append(page_text)

    return "\n".join(text)


def extract_text_from_excel(file_path: str) -> str:
    """
    Lit un fichier Excel et retourne une représentation textuelle
    de toutes les feuilles (style CSV).
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    sheets = pd.read_excel(path, sheet_name=None)

    chunks = []
    for sheet_name, df in sheets.items():
        chunks.append(f"=== Sheet: {sheet_name} ===")
        chunks.append(df.to_csv(index=False))
        chunks.append("")

    return "\n".join(chunks)


def extract_text_from_csv(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(path)
    return df.to_csv(index=False)


def extract_text_for_document_file(file_path: str) -> str:
    """
    Routage en fonction de l'extension du fichier local.
    """
    suffix = Path(file_path).suffix.lower()

    if suffix == ".pdf":
        return extract_text_from_pdf(file_path)
    elif suffix in [".xls", ".xlsx"]:
        return extract_text_from_excel(file_path)
    elif suffix == ".csv":
        return extract_text_from_csv(file_path)
    else:
        logging.info("Unsupported file extension for text extraction: %s", suffix)
        return ""


def process_document_for_text(document: Document, language: str = "FR") -> None:
    """
    Extrait le texte d'un document PDF/Excel/CSV et le sauvegarde
    en base. Les ZIP sont ignorés ici (contenu déjà traité).
    """
    suffix = Path(document.local_path).suffix.lower()

    if suffix == ".zip":
        logging.info("Skipping ZIP document (contents already handled): %s", document.filename)
        return

    logging.info("Extracting content for document id=%s, title=%s", document.id, document.title)
    text = extract_text_for_document_file(document.local_path)

    if not text.strip():
        logging.info("No extractable text for document id=%s", document.id)
        return

    save_document_text(
        document_id=document.id,
        language=language,
        is_original=True,
        text=text,
        summary=None,
    )
