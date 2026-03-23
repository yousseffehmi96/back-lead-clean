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
@router.get("/download-leads")
def download_leads(db: Session = Depends(get_db)):
        return SP.DowloadProdLead(db)
@router.post("/toblack/{id}")
def ToBlack(id:int,eliminer:str=Body(...),db: Session = Depends(get_db)):
        return SP.ToBlack(id,eliminer,db)
@router.get("/clean")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllClean(db)

@router.get("/stat")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllStat(db)

@router.post("/staging-dispatch")
async def StagingDispatch(filename: str = Body(...), db: Session = Depends(get_db)):
    try:
        result = {}
        r1=Ss.AddAuto(db)
        result.update(r1)
        r2 = CompleteEmail(db)
        result.update(r2)

        r3 = CheckContactsBlack(db)
        result.update(r3)

        r4 = SP.CompleteSocieteFromEmail(db)
        result.update(r4)

        r5 = SP.CompleteNomPrenomFromEmail(db)
        result.update(r5)

        db.expire_all()

        r6 = SP.StagingToGold(db)
        result.update(r6)

        r7 = SP.StagingToSilver(db)
        result.update(r7)

        r8 = SP.StagingToClean(db)
        result.update(r8)
        if filename:
                result["filename"] = filename
                updatestat(db, result)
        return result

    except Exception as e:
        import traceback
        print("ERREUR COMPLETE:", traceback.format_exc())  # ← affiche la vraie erreur
        raise HTTPException(status_code=500, detail=str(e))