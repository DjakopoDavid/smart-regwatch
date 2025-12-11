from datetime import datetime

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    BigInteger, ForeignKey, Boolean, Text
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DATABASE_URL = "sqlite:///regwatch.db"

Base = declarative_base()
engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, index=True)  # ex: BCL_AnaCredit, ECB_AnaCredit
    title = Column(String)
    url = Column(String, index=True)
    filename = Column(String)
    local_path = Column(String)
    filesize_bytes = Column(BigInteger)
    mime_type = Column(String)
    checksum = Column(String, index=True)
    version = Column(Integer, default=1)
    previous_version_id = Column(Integer, ForeignKey("documents.id"), nullable=True)
    previous_version = relationship("Document", remote_side=[id])

    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)
    downloaded_at = Column(DateTime, default=datetime.utcnow)

    texts = relationship("DocumentText", back_populates="document")
    keyword_matches = relationship("KeywordMatch", back_populates="document")
    notifications = relationship("NotificationLog", back_populates="document")


class DocumentText(Base):
    __tablename__ = "document_texts"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey("documents.id"), index=True)
    language = Column(String, index=True)  # "FR", "EN", ...
    is_original = Column(Boolean, default=True)
    full_text = Column(Text)
    summary = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="texts")
    keyword_matches = relationship("KeywordMatch", back_populates="text")


class KeywordConfig(Base):
    __tablename__ = "keyword_configs"

    id = Column(Integer, primary_key=True, index=True)
    keyword = Column(String, unique=True, index=True)
    category = Column(String, default="technical")  # technical / business
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class KeywordMatch(Base):
    __tablename__ = "keyword_matches"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey("documents.id"), index=True)
    text_id = Column(Integer, ForeignKey("document_texts.id"), index=True)
    keyword_id = Column(Integer, ForeignKey("keyword_configs.id"), index=True)

    occurrences = Column(Integer, default=1)
    context_snippet = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="keyword_matches")
    text = relationship("DocumentText", back_populates="keyword_matches")
    keyword = relationship("KeywordConfig")


class NotificationLog(Base):
    __tablename__ = "notification_logs"

    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey("documents.id"), index=True)
    subject = Column(String)
    recipients = Column(String)
    status = Column(String)  # SENT / ERROR
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    document = relationship("Document", back_populates="notifications")


def init_db():
    """Crée les tables si elles n'existent pas."""
    Base.metadata.create_all(bind=engine)


def upsert_document(meta: dict) -> Document:
    """
    Insère ou met à jour un document en fonction de l'URL et du checksum.
    - Si l'URL est inconnue -> nouveau document, version 1.
    - Si l'URL existe mais checksum différent -> nouvelle version.
    - Si l'URL existe et même checksum -> on met juste à jour last_seen_at.
    """
    session = SessionLocal()
    try:
        existing_docs = (
            session.query(Document)
            .filter(Document.url == meta["url"])
            .order_by(Document.version.desc())
            .all()
        )

        now = datetime.utcnow()

        if not existing_docs:
            # Nouveau document
            doc = Document(
                source=meta["source"],
                title=meta["title"],
                url=meta["url"],
                filename=meta["filename"],
                local_path=meta["local_path"],
                filesize_bytes=meta["filesize_bytes"],
                mime_type=meta["mime_type"],
                checksum=meta["checksum"],
                version=1,
                first_seen_at=now,
                last_seen_at=now,
                downloaded_at=meta["downloaded_at"],
            )
            session.add(doc)
            session.commit()
            session.refresh(doc)
            return doc

        latest = existing_docs[0]

        if latest.checksum == meta["checksum"]:
            # Même version, on met à jour la date de dernière vue
            latest.last_seen_at = now
            session.commit()
            session.refresh(latest)
            return latest

        # Nouvelle version
        new_version = latest.version + 1
        doc = Document(
            source=meta["source"],
            title=meta["title"],
            url=meta["url"],
            filename=meta["filename"],
            local_path=meta["local_path"],
            filesize_bytes=meta["filesize_bytes"],
            mime_type=meta["mime_type"],
            checksum=meta["checksum"],
            version=new_version,
            previous_version_id=latest.id,
            first_seen_at=now,
            last_seen_at=now,
            downloaded_at=meta["downloaded_at"],
        )
        session.add(doc)
        session.commit()
        session.refresh(doc)
        return doc
    finally:
        session.close()


# Helpers pour les autres agents

def save_document_text(document_id: int, language: str, is_original: bool,
                       text: str, summary: str | None = None) -> DocumentText:
    session = SessionLocal()
    try:
        dt = DocumentText(
            document_id=document_id,
            language=language,
            is_original=is_original,
            full_text=text,
            summary=summary,
        )
        session.add(dt)
        session.commit()
        session.refresh(dt)
        return dt
    finally:
        session.close()


def get_document_texts(document_id: int, language: str | None = None) -> list[DocumentText]:
    session = SessionLocal()
    try:
        q = session.query(DocumentText).filter(DocumentText.document_id == document_id)
        if language:
            q = q.filter(DocumentText.language == language)
        return q.order_by(DocumentText.created_at.desc()).all()
    finally:
        session.close()


def get_or_create_keyword(keyword: str, category: str = "technical") -> KeywordConfig:
    session = SessionLocal()
    try:
        kc = session.query(KeywordConfig).filter(KeywordConfig.keyword == keyword).first()
        if kc:
            return kc
        kc = KeywordConfig(keyword=keyword, category=category, is_active=True)
        session.add(kc)
        session.commit()
        session.refresh(kc)
        return kc
    finally:
        session.close()


def get_active_keywords() -> list[KeywordConfig]:
    session = SessionLocal()
    try:
        return session.query(KeywordConfig).filter(KeywordConfig.is_active == True).all()
    finally:
        session.close()


def save_keyword_match(document_id: int, text_id: int, keyword_id: int,
                       occurrences: int, context_snippet: str | None) -> KeywordMatch:
    session = SessionLocal()
    try:
        km = KeywordMatch(
            document_id=document_id,
            text_id=text_id,
            keyword_id=keyword_id,
            occurrences=occurrences,
            context_snippet=context_snippet,
        )
        session.add(km)
        session.commit()
        session.refresh(km)
        return km
    finally:
        session.close()


def log_notification(document_id: int, subject: str,
                     recipients: str, status: str, error_message: str | None = None) -> NotificationLog:
    session = SessionLocal()
    try:
        log = NotificationLog(
            document_id=document_id,
            subject=subject,
            recipients=recipients,
            status=status,
            error_message=error_message,
        )
        session.add(log)
        session.commit()
        session.refresh(log)
        return log
    finally:
        session.close()
