from fastapi import Form,UploadFile,File,APIRouter,Depends,Body
from service.service import *
from database.db import get_db
import service.serviceLeads as sp
Router=APIRouter()

@Router.post("/upload")
async def Upload(userid: str = Form(...), username: str | None = Form(None), file: UploadFile = File(...), db: Session = Depends(get_db)):
  
    stats = {} 
    print('usedis',userid)
    stats["filename"]=file.filename
    stats["iduser"]=userid
    stats.update(LoadFileToBd(file, db, userid, username))
    # si fichier déjà traité (détecté via historique), ne pas polluer staging/stat
    if stats.get("duplicate_file_processed"):
        return stats
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
    # Check: fichier déjà traité (doublons vs staging_leads) dès l'import
    try:
        inserted_rows = int(stats.get("inserted_rows", 0) or 0)
        already_processed = sp.CountLastImportAlreadyProcessedInApplique(db, file.filename, userid, inserted_rows)
        stats["already_processed_in_applique"] = already_processed
        if inserted_rows > 0 and already_processed == inserted_rows:
            stats["duplicate_file_processed"] = True
            stats["message"] = "Tu as deja traite ce fichier"
    except Exception:
        pass
    return stats


@Router.post("/upload-mapped")
async def UploadMapped(payload=Body(...), db: Session = Depends(get_db)):
    """Insère des lignes mappées (mapping manuel des colonnes) dans import_leads,
    exactement comme /upload le fait pour un fichier.

    - Si `mapping` est fourni, les `rows` sont des lignes brutes (entêtes du fichier)
      et le mapping {champ: entête} est appliqué ici, côté serveur.
    - Sinon, les `rows` sont supposées déjà mappées (clés canoniques)."""
    rows = payload.get("rows") or []
    mapping = payload.get("mapping") or {}
    if mapping:
        rows = ApplyFieldMapping(rows, mapping)
    userid = str(payload.get("userid") or "")
    username = payload.get("username")
    filename = payload.get("filename") or "import-mappe"

    stats = {}
    stats["filename"] = filename
    stats["iduser"] = userid
    stats.update(LoadRowsToBd(rows, db, userid, username, filename))
    if stats.get("duplicate_file_processed"):
        return stats
    nettoyer_contact(db)
    stats.setdefault("emails_completed", 0)
    stats.setdefault("blacklisted_removed", 0)
    stats.setdefault("moved_to_silver", 0)
    stats.setdefault("moved_to_gold", 0)
    stats.setdefault("moved_to_clean", 0)
    stats.setdefault("staging_vs_silver", 0)
    stats.setdefault("staging_vs_gold", 0)
    stats.setdefault("staging_internal", 0)
    static = Static(**stats)
    SaveStatic(db, static)
    try:
        inserted_rows = int(stats.get("inserted_rows", 0) or 0)
        already_processed = sp.CountLastImportAlreadyProcessedInApplique(db, filename, userid, inserted_rows)
        stats["already_processed_in_applique"] = already_processed
        if inserted_rows > 0 and already_processed == inserted_rows:
            stats["duplicate_file_processed"] = True
            stats["message"] = "Tu as deja traite ce fichier"
    except Exception:
        pass
    return stats



@Router.get("/import")
async def StagingLeads(db: Session = Depends(get_db)):
        """LoadFileToBd(file, db)"""
        return sp.GetAllStaging(db)
"""@Router.get("/clean")
async def clean"""