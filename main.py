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
app=FastAPI()
Base.metadata.create_all(bind=engine)
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



