from fastapi import APIRouter, Depends,Body
from sqlalchemy.orm import Session
import service.serviceLeads as SP
from database.db import get_db
from service.service import *
import service.serviceSociete as Ss
router=APIRouter()
@router.get("/silver")
async def GetAllsilver(db: Session = Depends(get_db)):
        return SP.GetAllSilver(db)
@router.get("/gold")
async def GetAllGold(db: Session = Depends(get_db)):
        return SP.GetAllGold(db)
@router.get("/black")
async def GetAllBlack(db: Session = Depends(get_db)):
        return SP.GetAllBlack(db)
@router.get("/download-leads-csv/{types}")
def download_leads(types:str,db: Session = Depends(get_db)):
        return SP.DownloadProdLeadCSV(types,db)
@router.get("/download-leads-xlsx/{types}")
def download_leads(types:str,db: Session = Depends(get_db)):
        return SP.DownloadLeadXlsx(types,db)
@router.post("/toblack/{id}")
def ToBlack(id:int,eliminer:str=Body(...),db: Session = Depends(get_db)):
        return SP.ToBlack(id,eliminer,db)
@router.get("/clean")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllClean(db)

@router.get("/stat")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllStat(db)

@router.post("/staging-dispatch/{base}")
async def StagingDispatch(base:str,filename: str = Body(...), db: Session = Depends(get_db)):
    try:
        result = {}
        
        # 1. Compléter les emails d'abord
        r2 = CompleteEmail(db,base)
        result.update(r2)
        
        # 2. Compléter societe + nom/prénom depuis les emails
        r4 = SP.CompleteSocieteFromEmail(db,base)
        result.update(r4)

        r5 = SP.CompleteNomPrenomFromEmail(db,base)
        result.update(db)

        # 3. Maintenant que societe est remplie → AddAuto fonctionne
        r1 = Ss.AddAuto(db,base)
        result.update(r1)
        
        # 4. Suite du pipeline
        r3 = CheckContactsBlack(db,base)
        result.update(r3)

        db.expire_all()

        r6 = SP.StagingToGold(db,base)
        result.update(r6)

        r7 = SP.StagingToSilver(db,base)
        result.update(r7)
        if(base=="staging_leads"):
                print("moved_to_cleaneeeeeeeee")
                r8 = SP.StagingToClean(db)
                result.update(r8)
        
        if filename:
            result["filename"] = filename
            updatestat(db, result)
            
        return result

    except Exception as e:
        import traceback
        print("ERREUR COMPLETE:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))