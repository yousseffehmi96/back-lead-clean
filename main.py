from fastapi import *
from api.api import Router as api_router
from api.apiSociete import routes as societe_router
from api.apiLeads import router as Leads_router
from api.apiToken import Route as Token_router
from model.staging_import_history import StagingImportHistory
from model.steaging_applique import SteagingApplique
from model.leads import Leads
from fastapi.middleware.cors import CORSMiddleware
from database.db import engine, Base
from api.apiValidationRules import routes as validation_rules_router
from service.serviceLeads import Rephrase
from sqlalchemy import text
app=FastAPI()

# Migration idempotente (AVANT create_all) : renommage des tables
#   staging_leads (brut import)   -> import_leads
#   steaging_applique (travail)   -> staging_leads
# Ordre strict : d'abord libérer le nom "staging_leads", puis y renommer l'applique.
with engine.begin() as conn:
    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='staging_leads')
               AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='import_leads')
               AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='steaging_applique') THEN
                ALTER TABLE staging_leads RENAME TO import_leads;
            END IF;
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='steaging_applique')
               AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='staging_leads') THEN
                ALTER TABLE steaging_applique RENAME TO staging_leads;
            END IF;
        END $$;
    """))

Base.metadata.create_all(bind=engine)

# Migration idempotente: societe_leads (domaine, extension) -> patterne
with engine.begin() as conn:
    conn.execute(text("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'societe_leads' AND column_name = 'domaine'
            ) THEN
                ALTER TABLE societe_leads ADD COLUMN IF NOT EXISTS patterne VARCHAR;
                UPDATE societe_leads
                   SET patterne = '{prenom}.{nom}@' || COALESCE(domaine, '') ||
                       CASE WHEN COALESCE(extension, '') <> '' THEN '.' || extension ELSE '' END
                 WHERE (patterne IS NULL OR patterne = '')
                   AND COALESCE(domaine, '') <> '';
                ALTER TABLE societe_leads DROP COLUMN IF EXISTS domaine;
                ALTER TABLE societe_leads DROP COLUMN IF EXISTS extension;
            ELSE
                ALTER TABLE societe_leads ADD COLUMN IF NOT EXISTS patterne VARCHAR;
            END IF;
        END $$;
    """))

# Migration idempotente: colonnes de leads varchar(n) -> TEXT (évite "value too long")
# Les données circulent depuis import_leads (TEXT) vers ces tables.
with engine.begin() as conn:
    conn.execute(text("""
        DO $$
        DECLARE r RECORD;
        BEGIN
            FOR r IN
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_name IN ('cleaning_leads','silver_leads','gold_leads','blacklist_leads')
                  AND column_name IN ('nom','prenom','email','fonction','societe','telephone')
                  AND data_type = 'character varying'
            LOOP
                EXECUTE format('ALTER TABLE %I ALTER COLUMN %I TYPE TEXT', r.table_name, r.column_name);
            END LOOP;
        END $$;
    """))

# Migration idempotente: colonne statu sur staging_leads (vérification email)
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE staging_leads ADD COLUMN IF NOT EXISTS statu TEXT"))

# Migration idempotente: colonne regex sur societe_leads (vérification du patterne)
with engine.begin() as conn:
    conn.execute(text("ALTER TABLE societe_leads ADD COLUMN IF NOT EXISTS regex VARCHAR"))

# Nettoyage idempotent des patternes: retirer un séparateur parasite avant '@'
# (ex. "{prenom}.{nom}.@axians.com" -> "{prenom}.{nom}@axians.com")
with engine.begin() as conn:
    conn.execute(text(r"""
        UPDATE societe_leads
        SET patterne = regexp_replace(patterne, '[._-]+@', '@', 'g')
        WHERE patterne ~ '[._-]+@'
    """))

# Migration idempotente : fusion silver_leads + gold_leads -> leads
# Le niveau de qualité devient un pourcentage (completion, 8 champs) ; Gold = 100%.
# Les anciennes tables sont CONSERVÉES en *_backup (aucun DROP de données) le temps de valider.

with engine.begin() as conn:
    def _table_exists(name: str) -> bool:
        return bool(conn.execute(
            text("SELECT 1 FROM information_schema.tables WHERE table_name = :n"),
            {"n": name},
        ).scalar())

    _cols = "email, nom, prenom, fonction, societe, telephone, linkedin, location, statu, created_at"
    # Gold en premier : en cas de doublon d'email, la ligne la plus complète gagne.
    for _src in ("gold_leads", "silver_leads"):
        if not _table_exists(_src):
            continue
        _n = conn.execute(text(f"SELECT COUNT(*) FROM {_src}")).scalar() or 0
        if _n:
            conn.execute(text(f"""
                INSERT INTO leads ({_cols})
                SELECT {_cols} FROM {_src}
                ON CONFLICT (email) DO NOTHING
            """))
        if _table_exists(f"{_src}_backup"):
            # Fusion déjà faite : table recréée vide par create_all -> on la retire
            if _n == 0:
                conn.execute(text(f"DROP TABLE {_src}"))
        else:
            conn.execute(text(f"ALTER TABLE {_src} RENAME TO {_src}_backup"))

    # La complétion n'est plus stockée : elle se calcule depuis les champs
    # (front pour l'affichage, sql_completion_expr côté SQL).
    conn.execute(text("ALTER TABLE leads DROP COLUMN IF EXISTS completion"))

from util.auth import require_auth
# Sécurité : toutes les routes exigent un token Clerk valide (si REQUIRE_AUTH=true)
_secured = [Depends(require_auth)]
app.include_router(api_router, dependencies=_secured)
app.include_router(societe_router, dependencies=_secured)
app.include_router(Leads_router, dependencies=_secured)
app.include_router(validation_rules_router, dependencies=_secured)
app.include_router(Token_router, dependencies=_secured)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://front-lead-clean.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



