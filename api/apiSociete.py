from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import service.serviceSociete as Sso
from database.db import get_db
from schema.schemaSociete import Societe

routes = APIRouter()

@routes.post("/societe")
async def ajout(societe: Societe, db: Session = Depends(get_db)):
    return Sso.AddSoc(societe, db)

@routes.delete("/societe/{id}")
async def delete(id: int, db: Session = Depends(get_db)):
    return Sso.DeleteSociete(id, db)

@routes.put("/societe/{id}")
async def update(id: int, societe: Societe, db: Session = Depends(get_db)):
    return Sso.UpdateSociete(id, societe, db)
@routes.get("/societe")
async def GetAll( db: Session = Depends(get_db)):
    return Sso.GetAll(db)