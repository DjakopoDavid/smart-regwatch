import streamlit as st

from storage.sqlite_db import (
    SessionLocal, Document, DocumentText,
    KeywordMatch, KeywordConfig, NotificationLog,
    get_or_create_keyword, get_active_keywords
)
from agents.extraction_agent import AUTHORITY_CONFIG
from agents.orchestrator import run_full_pipeline_for_authority


def get_session():
    return SessionLocal()


def load_documents_for_authority(source_label: str, limit: int = 100):
    session = get_session()
    try:
        docs = (
            session.query(Document)
            .filter(Document.source == source_label)
            .order_by(Document.downloaded_at.desc())
            .limit(limit)
            .all()
        )
        return docs
    finally:
        session.close()


def load_document_details(doc_id: int):
    session = get_session()
    try:
        doc = session.query(Document).get(doc_id)
        texts = (
            session.query(DocumentText)
            .filter(DocumentText.document_id == doc_id)
            .order_by(DocumentText.created_at.desc())
            .all()
        )
        matches = (
            session.query(KeywordMatch)
            .filter(KeywordMatch.document_id == doc_id)
            .all()
        )
        notifications = (
            session.query(NotificationLog)
            .filter(NotificationLog.document_id == doc_id)
            .order_by(NotificationLog.created_at.desc())
            .all()
        )
        return doc, texts, matches, notifications
    finally:
        session.close()


def add_business_keyword(keyword: str):
    if not keyword.strip():
        return
    get_or_create_keyword(keyword.strip(), category="business")


def main():
    st.set_page_config(page_title="Smart RegWatch - Datathon", layout="wide")

    st.title("Smart Regulatory Watch Tool (AnaCredit)")

    # Sélecteur d'autorité
    st.sidebar.header("Configuration")
    authority_codes = list(AUTHORITY_CONFIG.keys())
    authority_labels = {
        code: f"{code} - {AUTHORITY_CONFIG[code]['name']}"
        for code in authority_codes
    }

    selected_authority = st.sidebar.selectbox(
        "Autorité bancaire",
        authority_codes,
        format_func=lambda x: authority_labels[x],
    )

    source_label = AUTHORITY_CONFIG[selected_authority]["source_label"]

    if st.sidebar.button("Lancer le pipeline pour cette autorité"):
        with st.spinner(f"Exécution du pipeline pour {selected_authority}..."):
            run_full_pipeline_for_authority(selected_authority)
        st.success(f"Pipeline terminé pour {selected_authority}.")

    tab_docs, tab_keywords, tab_notifications = st.tabs(
        ["Documents", "Mots-clés", "Notifications"]
    )

    with tab_docs:
        st.subheader(f"Documents surveillés ({selected_authority})")

        docs = load_documents_for_authority(source_label, limit=200)
        if not docs:
            st.info("Aucun document en base pour cette autorité. "
                    "Lance d'abord le pipeline via le bouton dans la barre latérale.")
        else:
            doc_options = {f"[{d.source}] {d.title} (v{d.version})": d.id for d in docs}
            selected_label = st.selectbox("Sélectionne un document :", list(doc_options.keys()))
            selected_id = doc_options[selected_label]

            doc, texts, matches, notifications = load_document_details(selected_id)

            st.markdown(f"**Titre :** {doc.title}")
            st.markdown(f"**Source :** {doc.source}")
            st.markdown(f"**Autorité :** {authority_labels[selected_authority]}")
            st.markdown(f"**URL :** {doc.url}")
            st.markdown(f"**Version :** {doc.version}")
            st.markdown(f"**Checksum :** `{doc.checksum}`")
            st.markdown(f"**Fichier local :** `{doc.local_path}`")

            st.markdown("---")
            st.subheader("Contenus extraits (originaux et traductions)")

            for t in texts:
                lang_label = f"{t.language} ({'original' if t.is_original else 'traduit'})"
                with st.expander(lang_label, expanded=t.is_original):
                    if t.summary:
                        st.markdown(f"**Résumé :** {t.summary}")
                    st.text_area(
                        "Texte complet",
                        t.full_text[:20000],
                        height=300,
                        key=f"text_{t.id}",
                    )

            st.markdown("---")
            st.subheader("Mots-clés détectés")

            if not matches:
                st.info("Aucun mot-clé détecté pour ce document.")
            else:
                for m in matches:
                    st.markdown(f"- **{m.keyword.keyword}** (occurrences: {m.occurrences})")
                    if m.context_snippet:
                        st.code(m.context_snippet)

    with tab_keywords:
        st.subheader("Mots-clés configurés")

        active_keywords = get_active_keywords()
        st.write(f"Mots-clés actifs ({len(active_keywords)}) :")
        for k in active_keywords:
            st.markdown(f"- {k.keyword} ({k.category})")

        st.markdown("---")
        st.subheader("Ajouter un mot-clé business")

        new_kw = st.text_input("Nouveau mot-clé business")
        if st.button("Ajouter"):
            add_business_keyword(new_kw)
            st.success(f"Mot-clé '{new_kw}' ajouté (business).")
            st.experimental_rerun()

    with tab_notifications:
        st.subheader("Dernières notifications (toutes autorités)")

        session = get_session()
        try:
            logs = (
                session.query(NotificationLog)
                .order_by(NotificationLog.created_at.desc())
                .limit(100)
                .all()
            )
        finally:
            session.close()

        if not logs:
            st.info("Aucune notification enregistrée pour le moment.")
        else:
            for log in logs:
                st.markdown(f"### Notification ID {log.id}")
                st.markdown(f"- **Document ID :** {log.document_id}")
                st.markdown(f"- **Sujet :** {log.subject}")
                st.markdown(f"- **Destinataires :** {log.recipients}")
                st.markdown(f"- **Statut :** {log.status}")
                st.markdown(f"- **Date :** {log.created_at}")
                if log.error_message:
                    st.markdown(f"- **Erreur :** `{log.error_message}`")
                st.markdown("---")


if __name__ == "__main__":
    main()
