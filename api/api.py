from fastapi import Form,UploadFile,File,APIRouter,Depends
from service.service import *
from database.db import get_db
import service.serviceLeads as sp
Router=APIRouter()

@Router.post("/upload")
async def Upload(userid: str = Form(...),file: UploadFile = File(...),db: Session = Depends(get_db)):
  
    stats = {} 
    print('usedis',userid)
    stats["filename"]=file.filename
    stats["iduser"]=userid
    stats.update(LoadFileToBd(file, db))
    print("stat lena",stats)
    nettoyer_contact(db)
    print("stat lena3",stats)
    stats.setdefault("emails_completed", 0)
    stats.setdefault("blacklisted_removed", 0)
    stats.setdefault("moved_to_silver", 0)
    stats.setdefault("moved_to_gold", 0)
    stats.setdefault("moved_to_clean", 0)
    stats.setdefault("staging_vs_silver", 0)
    stats.setdefault("staging_vs_gold", 0)
    stats.setdefault("staging_internal", 0)
    static = Static(**stats) 
    print("hethi static")
    print("kikiki")
    SaveStatic(db,static)
    return stats
        


@Router.get("/staging")
async def StagingLeads(db: Session = Depends(get_db)):
        """LoadFileToBd(file, db)"""
        return sp.GetAllStaging(db)
"""@Router.get("/clean")
async def clean"""