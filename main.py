from fastapi import *
from api.api import Router as api_router
from api.apiSociete import routes as societe_router
from api.apiLeads import router as Leads_router
from fastapi.middleware.cors import CORSMiddleware
from database.db import engine, Base

app=FastAPI()
Base.metadata.create_all(bind=engine)
app.include_router(api_router)
app.include_router(societe_router)
app.include_router(Leads_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://front-lead-clean.vercel.app","http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


