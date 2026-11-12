from fastapi import APIRouter, Depends,Body
from sqlalchemy.orm import Session
import service.serviceLeads as SP
from database.db import get_db
from service.service import *
import service.serviceImportJob as SIJ
import service.serviceSociete as Ss
router=APIRouter()
@router.get("/optimized")
async def GetAllLeads(db: Session = Depends(get_db)):
        """Table unifiée. La complétion n'est pas stockée :
        le front la calcule à partir des 8 champs (100% => Complete)."""
        return SP.GetAllLeads(db)
@router.get("/incomplete")
async def GetAllIncomplete(db: Session = Depends(get_db)):
        """Rétro-compatible : vue filtrée des leads incomplets (< 100%)."""
        return SP.GetAllIncomplete(db)
@router.get("/complete")
async def GetAllComplete(db: Session = Depends(get_db)):
        """Rétro-compatible : vue filtrée des leads complets (100%)."""
        return SP.GetAllComplete(db)
@router.get("/black")
async def GetAllBlack(db: Session = Depends(get_db)):
        return SP.GetAllBlack(db)
@router.get("/download-leads-csv/{types}")
def download_leads(types:str,db: Session = Depends(get_db)):
        return SP.DownloadProdLeadCSV(types,db)
@router.get("/download-leads-xlsx/{types}")
def download_leads(types:str,db: Session = Depends(get_db)):
        return SP.DownloadLeadXlsx(types,db)
@router.get("/import/download-last-import-csv")
def download_last_staging_csv(userid: str, db: Session = Depends(get_db)):
        return SP.DownloadLatestStagingImportCSV(db, userid)
@router.get("/import/download-last-import-xlsx")
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
@router.get("/staging")
async def GetAllSteagingApplique(db: Session = Depends(get_db)):
        return SP.GetAllSteagingApplique(db)

@router.post("/staging/to-optimized")
async def SteagingAppliqueToOptimized(payload = Body(...), db: Session = Depends(get_db)):
        ids = []
        pattern = None
        if isinstance(payload, dict):
                ids = payload.get("ids") or []
                pattern = payload.get("pattern")
        return SP.SteagingAppliqueToOptimized(db, ids=ids, pattern=pattern)

@router.post("/staging/verify/{id}")
def verify_applique_lead(id: int, db: Session = Depends(get_db)):
        return SP.VerifyAppliqueLead(db, id)

@router.post("/staging/generate/{id}")
def generate_applique_email(id: int, db: Session = Depends(get_db)):
        return SP.GenerateAppliqueEmail(db, id)

@router.post("/staging/verify-bulk")
def verify_applique_bulk(payload = Body(...)):
        ids = payload.get("ids") or [] if isinstance(payload, dict) else []
        return SP.start_verify_job(ids)

@router.get("/staging/verify-status/{job_id}")
def verify_applique_status(job_id: str):
        return SP.get_verify_job(job_id)

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
def StagingDispatch(base:str,payload = Body(...), db: Session = Depends(get_db)):
    """Nettoyage, complétion, déduplication puis répartition vers staging_leads.

    Le corps vit dans service/serviceImportJob.run_dispatch_pipeline : le worker
    d'import en tâche de fond exécute exactement la même séquence, en y
    branchant en plus la progression. `def` et non `async def` : SQL synchrone,
    donc exécuté dans le threadpool de FastAPI et non sur la boucle d'événements.
    """
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

        return SIJ.run_dispatch_pipeline(
            db,
            base,
            filename=filename,
            userid=userid,
            inserted_rows=inserted_rows,
            email_pattern=email_pattern,
        )

    except Exception as e:
        import traceback
        print("ERREUR COMPLETE:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/export-leads/download-xlsx")
def download_export_leads_xlsx(filename: str | None = None, userid: str | None = None,
                               db: Session = Depends(get_db)):
    """Télécharge en Excel le résultat nettoyé d'un import (table export_leads).
    Sans `filename`, renvoie le dernier import de l'utilisateur."""
    return SP.DownloadExportLeadsXlsx(db, filename=filename, userid=userid)


@router.get("/tocomplete/{id}")
async def checkleadcomplete(id:int,db:Session=Depends(get_db)):
        return  SP.CheckLeadComplete(db,id)

@router.post("/optimized/complete-email")
async def complete_optimized_email(payload = Body(...), db: Session = Depends(get_db)):
    try:
        pattern = None
        overwrite = True
        if isinstance(payload, dict):
            pattern = payload.get("pattern")
            if "overwrite" in payload:
                overwrite = bool(payload.get("overwrite"))
        else:
            pattern = payload
        # 1) Appliquer le pattern sur optimized
        result = CompleteEmail(db, "optimized", pattern=pattern, overwrite=overwrite)
        # 2) Après application, enregistrer automatiquement les sociétés manquantes
        # (si societe existe dans optimized mais pas encore dans societe_leads)
        added = Ss.AddAuto(db, "optimized")
        if isinstance(added, dict) and "added_societes" in added:
            result["added_societes"] = int(added.get("added_societes", 0) or 0)
        return result
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print("ERREUR /optimized/complete-email:", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/optimized/email-collisions")
async def preview_optimized_email_collisions(
    pattern: str | None = None,
    overwrite: bool = True,
    limit_emails: int = 50,
    limit_leads_per_email: int = 20,
    db: Session = Depends(get_db),
):
    result = PreviewEmailCollisions(
        db,
        "optimized",
        pattern=pattern,
        overwrite=overwrite,
        limit_emails=limit_emails,
        limit_leads_per_email=limit_leads_per_email,
    )
    try:
        import json

        print("JSON /optimized/email-collisions:")
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

@router.put("/optimized/{id}/email")
async def update_optimized_email(id: int, payload = Body(...), db: Session = Depends(get_db)):
    # payload attendu: { "email": "..." }
    email = None
    if isinstance(payload, dict):
        email = payload.get("email")
    else:
        email = payload
    return SP.UpdateOptimizedEmail(db, id, str(email or ""))
def _ids_du_payload(payload) -> list[int]:
    ids = payload.get("ids") if isinstance(payload, dict) else None
    return [int(i) for i in (ids or []) if str(i).lstrip("-").isdigit()]


@router.post("/optimized/export-csv")
def export_leads_csv(payload = Body(...), db: Session = Depends(get_db)):
    """Exporte exactement les leads envoyés par le front (ex: ceux à 100%).
    La complétion étant calculée côté front, c'est lui qui fournit la sélection."""
    return SP.DownloadProdLeadCSV("optimized", db, ids=_ids_du_payload(payload))


@router.post("/optimized/export-xlsx")
def export_leads_xlsx(payload = Body(...), db: Session = Depends(get_db)):
    """Idem en XLSX."""
    return SP.DownloadLeadXlsx("optimized", db, ids=_ids_du_payload(payload))


@router.put("/optimized/{id}/field")
async def update_lead_field(id: int, payload = Body(...), db: Session = Depends(get_db)):
    """Édition inline d'un champ depuis le tableau Leads.
    payload attendu : { "field": "telephone", "value": "0601..." }
    Renvoie la nouvelle `completion` du lead (100 => Complete)."""
    field = ""
    value = None
    if isinstance(payload, dict):
        field = str(payload.get("field") or "")
        value = payload.get("value")
    return SP.UpdateLeadField(db, id, field, value)


@router.get("/teste/lead")
def faire(db:Session = Depends(get_db)):
    SP.Rephrase(db,"import_leads")


@router.get("/send/{email}")
def send_get(email: str, db: Session = Depends(get_db)):
    return SP.send_and_check(email, db)

@router.post("/send/bulk")
def send_bulk(emails: list[str] = Body(...), db: Session = Depends(get_db)):
    # Envoie tout le lot puis attend une seule fois les bounces
    # (une boucle sur send_and_check attendrait 20s par adresse).
    return SP.send_and_check_bulk(emails, db)


@router.get('/location/{base}')
def location(base: str, db: Session = Depends(get_db)):
    return SP.Rephrase(db, base)