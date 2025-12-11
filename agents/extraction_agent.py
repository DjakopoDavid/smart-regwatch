import os
import io
import hashlib
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path
import zipfile

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Configuration des autorités bancaires
AUTHORITY_CONFIG = {
    "BCL": {
        "name": "Banque centrale du Luxembourg",
        "source_label": "BCL_AnaCredit",
        "base_url": "https://www.bcl.lu",
        "listing_url": (
            "https://www.bcl.lu/en/Regulatory-reporting/"
            "Etablissements_credit/AnaCredit/Instructions/index.html"
        ),
    },
    "ECB": {
        "name": "European Central Bank",
        "source_label": "ECB_AnaCredit",
        # Exemple de page AnaCredit (à adapter au besoin)
        "base_url": "https://www.ecb.europa.eu",
        "listing_url": "https://www.ecb.europa.eu/stats/money_credit_banking/anacredit/html/index.en.html",
    },
}

SUPPORTED_EXTENSIONS = [".pdf", ".zip", ".xls", ".xlsx", ".csv"]


def get_file_checksum(content: bytes, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    h.update(content)
    return h.hexdigest()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def extract_document_links(authority_code: str) -> list[dict]:
    """
    Scrape la page de l'autorité et retourne la liste des liens
    vers des documents (pdf, xls, xlsx, csv, zip).
    """
    if authority_code not in AUTHORITY_CONFIG:
        raise ValueError(f"Unknown authority: {authority_code}")

    cfg = AUTHORITY_CONFIG[authority_code]
    base_url = cfg["base_url"]
    listing_url = cfg["listing_url"]

    logging.info("Fetching listing page for %s: %s", authority_code, listing_url)
    resp = requests.get(listing_url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    documents = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"):
            continue

        file_url = urljoin(base_url, href)
        ext = Path(urlparse(file_url).path).suffix.lower()

        if ext in SUPPORTED_EXTENSIONS:
            documents.append({
                "url": file_url,
                "title": a.get_text(strip=True) or os.path.basename(file_url),
            })

    logging.info("Found %d document links for %s", len(documents), authority_code)
    return documents


def _guess_mime_type_from_ext(ext: str) -> str:
    ext = ext.lower()
    if ext == ".pdf":
        return "application/pdf"
    if ext in [".xls", ".xlsx"]:
        return "application/vnd.ms-excel"
    if ext == ".csv":
        return "text/csv"
    if ext == ".zip":
        return "application/zip"
    return "application/octet-stream"


def _download_binary(url: str) -> tuple[bytes, dict]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content, resp.headers


def _build_meta(source_label: str, title: str, url: str,
                local_path: str, filename: str,
                content: bytes, headers: dict,
                downloaded_at: datetime) -> dict:
    checksum = get_file_checksum(content)
    filesize = len(content)
    mime_type = headers.get("Content-Type") or _guess_mime_type_from_ext(Path(filename).suffix)

    return {
        "source": source_label,
        "title": title,
        "url": url,
        "local_path": local_path,
        "filename": filename,
        "filesize_bytes": filesize,
        "checksum": checksum,
        "mime_type": mime_type,
        "downloaded_at": downloaded_at,
    }


def _handle_zip(meta_zip: dict, zip_content: bytes,
                download_dir: str) -> list[dict]:
    """
    Décompresse un ZIP et retourne une liste de métadonnées pour
    chaque fichier interne supporté (pdf, xls, xlsx, csv).
    """
    logging.info("Processing ZIP content: %s", meta_zip["filename"])
    metas: list[dict] = []
    zip_path = Path(meta_zip["local_path"])
    zip_stem = zip_path.stem

    unzip_dir = Path(download_dir) / f"unzipped_{zip_stem}"
    ensure_dir(unzip_dir.as_posix())

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue

            inner_name = Path(member.filename).name
            ext = Path(inner_name).suffix.lower()
            if ext not in SUPPORTED_EXTENSIONS:
                continue

            inner_bytes = zf.read(member)
            inner_local_path = unzip_dir / inner_name
            ensure_dir(inner_local_path.parent.as_posix())
            with open(inner_local_path, "wb") as f:
                f.write(inner_bytes)

            downloaded_at = meta_zip["downloaded_at"]

            # URL synthétique pour l'élément du ZIP
            inner_url = f"{meta_zip['url']}#{member.filename}"

            inner_meta = _build_meta(
                source_label=meta_zip["source"],
                title=f"{meta_zip['title']} - {inner_name}",
                url=inner_url,
                local_path=str(inner_local_path),
                filename=inner_name,
                content=inner_bytes,
                headers={"Content-Type": _guess_mime_type_from_ext(ext)},
                downloaded_at=downloaded_at,
            )
            metas.append(inner_meta)

    logging.info("Extracted %d inner files from ZIP %s", len(metas), meta_zip["filename"])
    return metas


def download_and_collect_metadata(authority_code: str,
                                  download_dir: str = "data/raw") -> list[dict]:
    """
    Pipeline d'extraction pour une autorité :
    - récupère les liens
    - télécharge chaque fichier
    - si ZIP, décompresse et ajoute les fichiers internes supportés
    - renvoie la liste des métadonnées pour tous les fichiers exploitables
    """
    if authority_code not in AUTHORITY_CONFIG:
        raise ValueError(f"Unknown authority: {authority_code}")

    cfg = AUTHORITY_CONFIG[authority_code]
    source_label = cfg["source_label"]

    ensure_dir(download_dir)

    docs = extract_document_links(authority_code)
    all_meta: list[dict] = []

    for doc in docs:
        url = doc["url"]
        title = doc["title"]
        logging.info("[%s] Downloading %s", authority_code, url)

        content, headers = _download_binary(url)
        downloaded_at = datetime.utcnow()

        url_path = urlparse(url).path
        filename = os.path.basename(url_path)
        ext = Path(filename).suffix.lower()

        local_path = os.path.join(download_dir, filename)
        with open(local_path, "wb") as f:
            f.write(content)

        meta = _build_meta(
            source_label=source_label,
            title=title,
            url=url,
            local_path=local_path,
            filename=filename,
            content=content,
            headers=headers,
            downloaded_at=downloaded_at,
        )

        all_meta.append(meta)

        if ext == ".zip":
            inner_metas = _handle_zip(meta, content, download_dir)
            all_meta.extend(inner_metas)

    logging.info("Total collected files (including zip contents) for %s: %d",
                 authority_code, len(all_meta))
    return all_meta


def run_extraction(authority_code: str,
                   download_dir: str = "data/raw") -> list[dict]:
    """
    Fonction externe appelée par l'orchestrateur.
    """
    return download_and_collect_metadata(authority_code, download_dir=download_dir)
