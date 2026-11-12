from fastapi import APIRouter, Body, HTTPException, Response, status

import service.serviceImportJob as SIJ

router = APIRouter()


@router.post("/import/start", status_code=status.HTTP_202_ACCEPTED)
def StartImport(response: Response, payload=Body(...)):
    """Démarre l'import en tâche de fond et rend la main tout de suite.

    Même corps que /upload-mapped (mapping, rows, userid, username, filename).
    Le front reçoit un job_id et suit la progression via /import/status.
    """
    job = SIJ.start_import_job(payload if isinstance(payload, dict) else {})
    if job.get("error") == "already_running":
        raise HTTPException(
            status_code=409,
            detail=f"Un import est déjà en cours ({job.get('filename') or 'fichier inconnu'}). "
                   "Attends la fin avant d'en lancer un autre.",
        )
    return job


@router.get("/import/status/{job_id}")
def ImportStatus(job_id: str):
    return SIJ.get_import_job(job_id)


@router.get("/import/current")
def CurrentImport(userid: str = ""):
    """Job en cours (ou dernier de moins d'une heure) pour cet utilisateur.

    Permet au front de se rattacher à un import après un rechargement de page,
    même si le job_id mémorisé localement a été perdu.
    """
    return SIJ.get_current_import_job(userid or None) or {"status": "none"}
