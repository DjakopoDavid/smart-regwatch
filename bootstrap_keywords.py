from storage.sqlite_db import init_db
from agents.keyword_agent import bootstrap_default_keywords

if __name__ == "__main__":
    init_db()
    bootstrap_default_keywords()
    print("Mots-clés techniques par défaut initialisés.")
