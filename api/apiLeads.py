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
@router.get("/staging/download-last-import-csv")
def download_last_staging_csv(userid: str, db: Session = Depends(get_db)):
        return SP.DownloadLatestStagingImportCSV(db, userid)
@router.get("/staging/download-last-import-xlsx")
def download_last_staging_xlsx(userid: str, db: Session = Depends(get_db)):
        return SP.DownloadLatestStagingImportXlsx(db, userid)
@router.post("/toblack/{id}")
def ToBlack(id:int,eliminer:str=Body(...),db: Session = Depends(get_db)):
        print("lena")
        return SP.ToBlack(id,eliminer,db)
@router.get("/clean")
async def GetAllClean(db:Session=Depends(get_db)):
        return SP.GetAllClean(db)

@router.post("/clean/delete")
async def DeleteClean(payload = Body(...), db: Session = Depends(get_db)):
        ids = []
        if isinstance(payload, dict):
                ids = payload.get("ids") or []
        return SP.DeleteCleanByIds(db, ids=ids)
@router.get("/steaging-applique")
async def GetAllSteagingApplique(db: Session = Depends(get_db)):
        return SP.GetAllSteagingApplique(db)

@router.post("/steaging-applique/to-silver")
async def SteagingAppliqueToSilver(payload = Body(...), db: Session = Depends(get_db)):
        ids = []
        pattern = None
        if isinstance(payload, dict):
                ids = payload.get("ids") or []
                pattern = payload.get("pattern")
        return SP.SteagingAppliqueToSilver(db, ids=ids, pattern=pattern)

@router.get("/staging-import-history")
async def GetStagingImportHistory(userid: str | None = None, is_manager: bool = False, db: Session = Depends(get_db)):
        if not is_manager and not userid:
                return []
        return SP.GetAllStagingImportHistory(db, None if is_manager else userid)

@router.get("/stat")
async def GetAllClean(userid: str | None = None, is_manager: bool = False, db:Session=Depends(get_db)):
        return SP.GetAllStat(db, userid=userid, is_manager=is_manager)

@router.get("/export/database-zip")
async def export_database_zip(is_manager: bool = False, db: Session = Depends(get_db)):
        return SP.ExportDatabaseZip(db, is_manager=is_manager)

@router.post("/staging-dispatch/{base}")
async def StagingDispatch(base:str,payload = Body(...), db: Session = Depends(get_db)):
    try:
        filename = ""
        userid = ""
        inserted_rows = 0
        email_pattern = None
        if isinstance(payload, dict):
            filename = payload.get("filename", "") or ""
            userid = payload.get("userid", "") or ""
            inserted_rows = int(payload.get("inserted_rows", 0) or 0)
            email_pattern = payload.get("email_pattern") or payload.get("pattern")
        else:
            filename = str(payload or "")

        result = {}
        r2 = CompleteEmail(db,base, pattern=email_pattern)
        result.update(r2)
        
        r4 = SP.CompleteSocieteFromEmail(db,base)
        result.update(r4)

        r5 = SP.CompleteNomPrenomFromEmail(db,base)
        result.update(r5)

        result.update(SupprimerDoublons(db))
        
        r3 = CheckContactsBlack(db,base)
        result.update(r3)

        # Total supprimé = doublons (silver/gold/applique/interne) + blacklistés retirés
        try:
            result["total_deleted"] = (
                int(result.get("staging_vs_silver", 0) or 0)
                + int(result.get("staging_vs_gold", 0) or 0)
                + int(result.get("staging_vs_applique", 0) or 0)
                + int(result.get("staging_internal", 0) or 0)
                + int(result.get("blacklisted_removed", 0) or 0)
            )
        except Exception:
            pass

        db.expire_all()

        r6 = SP.StagingToGold(db,base)
        
        result.update(r6)

        r7 = SP.StagingToSilver(db,base)
        result.update(r7)
        if(base=="staging_leads"):
                print("moved_to_cleaneeeeeeeee")
                r8 = SP.StagingToClean(db)
                result.update(r8)
                r9 = SP.StagingToSteagingApplique(db, base)
                result.update(r9)
                # Sécurité: vider staging quoi qu'il reste après dispatch
                r10 = SP.ClearBaseTable(db, base)
                result.update(r10)
        
        if filename:
            result["filename"] = filename
            total_deleted = int(result.get("total_deleted", 0) or 0)
            moved_to_applique = int(result.get("moved_to_steaging_applique", 0) or 0)
            handled_as_already_processed = total_deleted + moved_to_applique
            if inserted_rows > 0 and handled_as_already_processed == inserted_rows and userid:
                rollback_result = rollback_duplicate_upload_records(db, filename, userid, inserted_rows)
                result.update(rollback_result)
                result["duplicate_file_processed"] = True
                result["message"] = "Tu as deja traite ce fichier"
            else:
                updatestat(db, result)
            
        return result

    except Exception as e:
        import traceback
        print("ERREUR COMPLETE:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/togold/{id}")
async def silvertogold(id:int,db:Session=Depends(get_db)):
        return  SP.SilverToGold(db,id)

@router.post("/silver/complete-email")
async def complete_silver_email(payload = Body(...), db: Session = Depends(get_db)):
    try:
        pattern = None
        overwrite = True
        if isinstance(payload, dict):
            pattern = payload.get("pattern")
            if "overwrite" in payload:
                overwrite = bool(payload.get("overwrite"))
        else:
            pattern = payload
        # 1) Appliquer le pattern sur silver_leads
        result = CompleteEmail(db, "silver_leads", pattern=pattern, overwrite=overwrite)
        # 2) Après application, enregistrer automatiquement les sociétés manquantes
        # (si societe existe dans silver_leads mais pas encore dans societe_leads)
        added = Ss.AddAuto(db, "silver_leads")
        if isinstance(added, dict) and "added_societes" in added:
            result["added_societes"] = int(added.get("added_societes", 0) or 0)
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print("ERREUR /silver/complete-email:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/silver/email-collisions")
async def preview_silver_email_collisions(
    pattern: str | None = None,
    overwrite: bool = True,
    limit_emails: int = 50,
    limit_leads_per_email: int = 20,
    db: Session = Depends(get_db),
):
    result = PreviewEmailCollisions(
        db,
        "silver_leads",
        pattern=pattern,
        overwrite=overwrite,
        limit_emails=limit_emails,
        limit_leads_per_email=limit_leads_per_email,
    )
    try:
        import json

        print("JSON /silver/email-collisions:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception:
        # Ne pas bloquer la réponse si l'affichage échoue
        pass
    return result

@router.get("/settings/email-pattern")
async def get_email_pattern(db: Session = Depends(get_db)):
    return {"pattern": GetEmailPattern(db)}

@router.put("/settings/email-pattern")
async def save_email_pattern(payload = Body(...), db: Session = Depends(get_db)):
    pattern = None
    is_manager = False
    if isinstance(payload, dict):
        pattern = payload.get("pattern")
        is_manager = bool(payload.get("is_manager", False))
    else:
        pattern = payload
    return SaveEmailPattern(db, str(pattern or ""), is_manager=is_manager)

@router.put("/silver/{id}/email")
async def update_silver_email(id: int, payload = Body(...), db: Session = Depends(get_db)):
    # payload attendu: { "email": "..." }
    email = None
    if isinstance(payload, dict):
        email = payload.get("email")
    else:
        email = payload
    return SP.UpdateSilverEmail(db, id, str(email or ""))
@router.get("/teste/lead")
def faire(db:Session = Depends(get_db)):
    SP.Rephrase(db,"staging_leads")