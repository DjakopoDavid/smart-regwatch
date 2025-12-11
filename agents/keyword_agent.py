import logging
import re
from textwrap import shorten

from storage.sqlite_db import (
    get_active_keywords, get_or_create_keyword,
    save_keyword_match, DocumentText
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


DEFAULT_TECH_KEYWORDS = [
    "AnaCredit",
    "credit institution",
    "Central Bank",
    "counterparty",
    "exposure",
    "loan",
    "Basel",
    "EBA",
    "European Banking Authority",
    "large exposure",
    "default",
    "non-performing",
]


def bootstrap_default_keywords():
    """
    Insère une liste de mots-clés techniques si elle n'existe pas déjà.
    """
    for kw in DEFAULT_TECH_KEYWORDS:
        get_or_create_keyword(kw, category="technical")
    logging.info("Default technical keywords bootstrapped.")


def find_keyword_occurrences(text: str, keyword: str) -> tuple[int, str | None]:
    """
    Compte les occurrences approximatives d'un mot-clé (case-insensitive)
    et retourne un extrait de contexte pour la première occurrence.
    """
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    matches = list(pattern.finditer(text))
    count = len(matches)
    if count == 0:
        return 0, None

    first = matches[0]
    start = max(0, first.start() - 200)
    end = min(len(text), first.end() + 200)
    snippet = text[start:end]
    snippet = shorten(snippet, width=400, placeholder="...")
    return count, snippet


def analyse_document_text(text_row: DocumentText) -> None:
    """
    Analyse un DocumentText pour tous les mots-clés actifs
    et enregistre les matches.
    """
    logging.info("Analysing keywords for document_text id=%s", text_row.id)

    text = text_row.full_text or ""
    if not text.strip():
        logging.info("Empty text, skipping.")
        return

    active_keywords = get_active_keywords()
    for kw in active_keywords:
        count, snippet = find_keyword_occurrences(text, kw.keyword)
        if count > 0:
            save_keyword_match(
                document_id=text_row.document_id,
                text_id=text_row.id,
                keyword_id=kw.id,
                occurrences=count,
                context_snippet=snippet,
            )
