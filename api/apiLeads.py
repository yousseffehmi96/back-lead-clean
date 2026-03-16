from fastapi import APIRouter, Depends,Body
from sqlalchemy.orm import Session
import service.serviceLeads as SP
from database.db import get_db

router=APIRouter()

@router.get("/prod")
async def GetAllProd(db: Session = Depends(get_db)):
        return SP.GetAllProd(db)
@router.get("/black")
async def GetAllBlack(db: Session = Depends(get_db)):
        return SP.GetAllBlack(db)
@router.get("/download-leads")
def download_leads(db: Session = Depends(get_db)):
        return SP.DowloadProdLead(db)
@router.post("/toblack/{id}")
def ToBlack(id:int,eliminer:str=Body(...),db: Session = Depends(get_db)):
        return SP.ToBlack(id,eliminer,db)
@router.get("/clean")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllClean(db)