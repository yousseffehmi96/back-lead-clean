from fastapi import UploadFile,File,APIRouter,Depends
from service.service import *
from database.db import get_db

Router=APIRouter()

@Router.post("/upload")
async def Upload(file: UploadFile = File(...), db: Session = Depends(get_db)):
    stats = {}  
    stats.update(LoadFileToBd(file, db)) 
    stats.update(SupprimerDoublons(db,"staging_leads"))  
    stats.update(CompleteEmail(db)) 
    stats.update(CheckContactsBlack(db))
    #nettoyer_contact(db) 
    stats.update(StagingToProd(db))
    return stats

