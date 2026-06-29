from fastapi import *
from api.api import Router as api_router
from api.apiSociete import routes as societe_router
from api.apiLeads import router as Leads_router
from api.apiToken import Route as Token_router
from model.staging_import_history import StagingImportHistory
from model.steaging_applique import SteagingApplique
from fastapi.middleware.cors import CORSMiddleware
from database.db import engine, Base
from api.apiValidationRules import routes as validation_rules_router
from service.serviceLeads import Rephrase
from sqlalchemy import text
app=FastAPI()
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
# Les données circulent depuis staging_leads (TEXT) vers ces tables.
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

app.include_router(api_router)
app.include_router(societe_router)
app.include_router(Leads_router)
app.include_router(validation_rules_router)
app.include_router(Token_router)
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



