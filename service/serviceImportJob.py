"""Import de leads en tâche de fond, avec progression consultable.

Pourquoi : l'import enchaînait deux requêtes HTTP bloquantes (/upload-mapped
puis /staging-dispatch/import_leads). Sur un gros fichier, le navigateur
attendait plusieurs minutes sans aucun retour, et quitter la page perdait tout.

Le patron est celui déjà en place pour la vérification d'emails
(`start_verify_job` / `get_verify_job`, serviceLeads.py) : un thread démon +
un identifiant de job que le front interroge. Deux différences :

- l'état est persisté dans la table `import_jobs` au lieu d'un dict en mémoire
  (survit à un redémarrage, fonctionne avec plusieurs workers) ;
- on n'utilise surtout pas VERIFY_JOBS : `run_auto_verify_cron` y supprime
  chaque minute tout job terminé, le résultat de l'import disparaîtrait avant
  que le front ait pu le lire.

Le corps des deux endpoints historiques est extrait ici (`run_upload_pipeline`
et `run_dispatch_pipeline`) afin que les endpoints synchrones et le worker de
fond partagent exactement la même logique — rien n'est dupliqué.
"""

import json
import threading
import uuid
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from database.db import SessionLocal
from model.import_job import ImportJob
from schema.schemaStatic import Static
import service.serviceLeads as SP
from service.service import (
    ApplyFieldMapping,
    CheckContactsBlack,
    CleanSpecialChars,
    CompleteEmail,
    LoadRowsToBd,
    SaveStatic,
    SupprimerDoublons,
    rollback_duplicate_upload_records,
    updatestat,
)

# Un job "running" qui n'a pas été mis à jour depuis ce délai est considéré
# comme interrompu (process tué, redéploiement…) : sans ça le front resterait
# bloqué à 10 % indéfiniment.
STALE_AFTER = timedelta(minutes=10)

# Démarrages sérialisés : le pipeline travaille sur TOUTE la table
# import_leads, pas seulement sur le lot courant. Deux imports simultanés
# mélangeraient leurs lignes.
_START_LOCK = threading.Lock()

# (libellé affiché, poids dans la barre de progression)
STEPS = [
    ("Préparation du fichier", 5),
    ("Insertion des lignes", 25),
    ("Nettoyage des champs", 20),
    ("Complétion des emails", 10),
    ("Complétion des sociétés", 5),
    ("Suppression des doublons", 10),
    ("Filtrage de la blacklist", 5),
    ("Transfert vers Clean", 5),
    ("Instantané pour l'export", 5),
    ("Transfert vers Staging", 5),
    ("Mise à jour des statistiques", 5),
]
_TOTAL_WEIGHT = sum(w for _, w in STEPS)


# ---------------------------------------------------------------------------
# Progression
# ---------------------------------------------------------------------------

def _percent_before(step_index: int) -> int:
    """Pourcentage atteint avant d'entamer l'étape `step_index`."""
    done_weight = sum(w for _, w in STEPS[:step_index])
    return int(round(done_weight * 100 / _TOTAL_WEIGHT))


class _Progress:
    """Écrit la progression du job dans `import_jobs`.

    Utilise sa propre session courte à chaque écriture : la session du worker
    est occupée par le pipeline (transactions longues), et le pool est petit
    (pool_size=5, max_overflow=10) — on ne garde donc rien ouvert.
    """

    def __init__(self, job_id: str, total: int):
        self.job_id = job_id
        self.total = total

    def step(self, index: int, done: int | None = None):
        label = STEPS[index][0] if index < len(STEPS) else "Finalisation"
        self._write(
            step=index,
            label=label,
            percent=_percent_before(index),
            done=done,
        )
        print(f"[import {self.job_id[:8]}] {_percent_before(index)}% - {label}")

    def _write(self, **fields):
        db = SessionLocal()
        try:
            values = {k: v for k, v in fields.items() if v is not None}
            values["updated_at"] = datetime.utcnow()
            sets = ", ".join(f"{k} = :{k}" for k in values)
            db.execute(
                text(f"UPDATE import_jobs SET {sets} WHERE job_id = :job_id"),
                {**values, "job_id": self.job_id},
            )
            db.commit()
        except Exception as e:  # une écriture de progression ne doit jamais casser l'import
            db.rollback()
            print(f"[import] progression non enregistrée : {e}")
        finally:
            db.close()

    def finish(self, result: dict):
        self._write(
            status="done",
            step=len(STEPS),
            label="Terminé",
            percent=100,
            done=self.total,
            result=json.dumps(result, default=str),
            finished_at=datetime.utcnow(),
        )

    def fail(self, error: str):
        self._write(
            status="error",
            label="Échec",
            error=str(error)[:2000],
            finished_at=datetime.utcnow(),
        )


# ---------------------------------------------------------------------------
# Pipeline — extrait des endpoints, partagé avec eux
# ---------------------------------------------------------------------------

def run_upload_pipeline(db: Session, rows, mapping, userid, username, filename) -> dict:
    """Mapping des colonnes + insertion dans import_leads + ligne de statistiques.

    Corps de `POST /upload-mapped`. `nettoyer_contact` n'est plus appelé ici :
    LoadRowsToBd nettoie déjà les mêmes champs (pandas, avant insertion), et la
    version ORM re-nettoyait toute la table ligne par ligne.
    """
    if mapping:
        rows = ApplyFieldMapping(rows, mapping)

    stats = {"filename": filename, "iduser": str(userid or "")}
    stats.update(LoadRowsToBd(rows, db, userid, username, filename))
    if stats.get("duplicate_file_processed"):
        return stats

    for key in (
        "emails_completed",
        "blacklisted_removed",
        "moved_to_incomplete",
        "moved_to_complete",
        "moved_to_clean",
        "staging_vs_incomplete",
        "staging_vs_complete",
        "staging_internal",
    ):
        stats.setdefault(key, 0)

    SaveStatic(db, Static(**stats))

    try:
        inserted_rows = int(stats.get("inserted_rows", 0) or 0)
        already = SP.CountLastImportAlreadyProcessedInApplique(db, filename, userid, inserted_rows)
        stats["already_processed_in_applique"] = already
        if inserted_rows > 0 and already == inserted_rows:
            stats["duplicate_file_processed"] = True
            stats["message"] = "Tu as deja traite ce fichier"
    except Exception:
        pass

    return stats


def run_dispatch_pipeline(
    db: Session,
    base: str,
    filename: str = "",
    userid: str = "",
    inserted_rows: int = 0,
    email_pattern=None,
    progress: "_Progress | None" = None,
    first_step: int = 2,
) -> dict:
    """Nettoyage, complétion, déduplication puis répartition vers staging_leads.

    Corps de `POST /staging-dispatch/{base}`. `progress` est optionnel : les
    endpoints synchrones l'omettent, le worker de fond le fournit pour que
    chaque étape fasse avancer la barre.
    """
    def mark(offset: int):
        if progress:
            progress.step(first_step + offset)

    result = {}

    mark(0)
    result.update(CleanSpecialChars(db, base))

    mark(1)
    result.update(CompleteEmail(db, base, pattern=email_pattern))

    mark(2)
    result.update(SP.CompleteSocieteFromEmail(db, base))

    mark(3)
    result.update(SupprimerDoublons(db))

    mark(4)
    result.update(CheckContactsBlack(db, base))

    # Total supprimé = doublons (incomplete/complete/applique/interne) + blacklistés retirés
    try:
        result["total_deleted"] = (
            int(result.get("staging_vs_incomplete", 0) or 0)
            + int(result.get("staging_vs_complete", 0) or 0)
            + int(result.get("staging_vs_applique", 0) or 0)
            + int(result.get("staging_internal", 0) or 0)
            + int(result.get("blacklisted_removed", 0) or 0)
        )
    except Exception:
        pass

    db.expire_all()

    if base == "import_leads":
        # Contacts trop incomplets (email+société vides, ou >=3 champs vides) -> Clean
        mark(5)
        result.update(SP.MoveIncompleteToClean(db, base))

        # Instantané du lot nettoyé AVANT que StagingToSteagingApplique ne vide
        # {base} : c'est ce lot qui sera proposé au téléchargement Excel.
        mark(6)
        result.update(SP.SnapshotToExportLeads(db, base, filename, userid))

        mark(7)
        result.update(SP.StagingToSteagingApplique(db, base))

        # Des leads viennent d'arriver en staging : vérification des emails sans
        # statut déclenchée en arrière-plan (job séparé, déjà asynchrone).
        verify_job = SP.trigger_auto_verify_unverified_staging(db)
        result["auto_verify_job_id"] = verify_job.get("job_id")
        result["auto_verify_total"] = verify_job.get("total")

    mark(8)
    if filename:
        result["filename"] = filename
        # "Déjà traité" UNIQUEMENT si TOUTES les lignes importées étaient des
        # doublons (un transfert vers staging est un succès normal).
        total_deleted = int(result.get("total_deleted", 0) or 0)
        if inserted_rows > 0 and total_deleted == inserted_rows and userid:
            result.update(rollback_duplicate_upload_records(db, filename, userid, inserted_rows))
            result["duplicate_file_processed"] = True
            result["message"] = "Tu as deja traite ce fichier"
        else:
            # iduser transmis pour que la mise à jour ne touche que la ligne de
            # statistiques de cet utilisateur (même nom de fichier possible
            # chez un autre agent)
            updatestat(db, {**result, "iduser": str(userid or "")})

    return result


# ---------------------------------------------------------------------------
# Job de fond
# ---------------------------------------------------------------------------

def _run_import_job(job_id: str, rows, mapping, userid, username, filename):
    """Exécute l'import complet dans un thread, en publiant sa progression."""
    progress = _Progress(job_id, total=len(rows or []))
    db = SessionLocal()
    try:
        progress.step(0)
        stats = run_upload_pipeline(db, rows, mapping, userid, username, filename)

        if stats.get("duplicate_file_processed"):
            progress.finish(stats)
            return

        progress.step(1, done=int(stats.get("inserted_rows", 0) or 0))

        result = run_dispatch_pipeline(
            db,
            "import_leads",
            filename=filename,
            userid=str(userid or ""),
            inserted_rows=int(stats.get("inserted_rows", 0) or 0),
            progress=progress,
            first_step=2,
        )
        progress.finish({**stats, **result})

    except Exception as e:
        import traceback
        print("[import] ECHEC:", traceback.format_exc())
        try:
            db.rollback()
        except Exception:
            pass
        progress.fail(getattr(e, "detail", None) or str(e))
    finally:
        db.close()


def start_import_job(payload: dict) -> dict:
    """Crée le job, lance le thread, rend la main immédiatement.

    Renvoie {"job_id", "total"}, ou {"error": "already_running"} si un import
    est déjà en cours (l'appelant en fait un 409).
    """
    rows = payload.get("rows") or []
    mapping = payload.get("mapping") or {}
    userid = str(payload.get("userid") or "")
    username = payload.get("username")
    filename = payload.get("filename") or "import-mappe"

    with _START_LOCK:
        running = get_current_import_job(userid=None, only_running=True)
        if running:
            return {
                "error": "already_running",
                "job_id": running.get("job_id"),
                "filename": running.get("filename"),
            }

        job_id = uuid.uuid4().hex
        db = SessionLocal()
        try:
            db.add(
                ImportJob(
                    job_id=job_id,
                    filename=filename,
                    iduser=userid,
                    status="running",
                    step=0,
                    label=STEPS[0][0],
                    percent=0,
                    done=0,
                    total=len(rows),
                )
            )
            db.commit()
        finally:
            db.close()

    threading.Thread(
        target=_run_import_job,
        args=(job_id, rows, mapping, userid, username, filename),
        daemon=True,
    ).start()

    return {"job_id": job_id, "total": len(rows)}


def _as_dict(job: ImportJob) -> dict:
    out = {
        "job_id": job.job_id,
        "filename": job.filename,
        "iduser": job.iduser,
        "status": job.status,
        "step": job.step or 0,
        "steps_total": len(STEPS),
        "label": job.label,
        "percent": job.percent or 0,
        "done": job.done or 0,
        "total": job.total or 0,
        "error": job.error,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
    }
    if job.result:
        try:
            out["result"] = json.loads(job.result)
        except Exception:
            out["result"] = None

    # Job resté "running" sans nouvelle : le worker a été interrompu.
    if job.status == "running" and job.updated_at and datetime.utcnow() - job.updated_at > STALE_AFTER:
        out["status"] = "error"
        out["error"] = "Import interrompu (serveur redémarré ?)"
    return out


def get_import_job(job_id: str) -> dict:
    db = SessionLocal()
    try:
        job = db.query(ImportJob).filter(ImportJob.job_id == job_id).first()
        return _as_dict(job) if job else {"status": "unknown"}
    finally:
        db.close()


def get_current_import_job(userid: str | None = None, only_running: bool = False) -> dict | None:
    """Job en cours, sinon le plus récent de moins d'une heure.

    Sert au front à se rattacher à un import après un rechargement de page,
    même sans job_id mémorisé.
    """
    db = SessionLocal()
    try:
        q = db.query(ImportJob)
        if userid:
            q = q.filter(ImportJob.iduser == str(userid))

        running = q.filter(ImportJob.status == "running").order_by(ImportJob.created_at.desc()).first()
        if running:
            data = _as_dict(running)
            # Un job périmé ne bloque pas un nouveau démarrage.
            if data["status"] == "running":
                return data
            _mark_stale(running.job_id)
            if only_running:
                return None
            return data
        if only_running:
            return None

        recent = (
            q.filter(ImportJob.created_at >= datetime.utcnow() - timedelta(hours=1))
            .order_by(ImportJob.created_at.desc())
            .first()
        )
        return _as_dict(recent) if recent else None
    finally:
        db.close()


def _mark_stale(job_id: str):
    db = SessionLocal()
    try:
        db.execute(
            text(
                "UPDATE import_jobs SET status = 'error', error = :err, finished_at = :now "
                "WHERE job_id = :job_id AND status = 'running'"
            ),
            {"err": "Import interrompu (serveur redémarré ?)", "now": datetime.utcnow(), "job_id": job_id},
        )
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()
