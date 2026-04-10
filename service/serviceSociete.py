from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe
from model.staging_leads import StagingLeads
from sqlalchemy import func,text
from sqlalchemy.exc import SQLAlchemyError

def AddSoc(societe: Societe, db: Session):
    """
    Crée une société manuellement via POST /societe.
    """
    try:
        data = societe.dict()
        nom = (data.get("nom") or "").strip()
        domaine = (data.get("domaine") or "").strip()
        extension = (data.get("extension") or "").strip()

        if not nom:
            raise HTTPException(status_code=400, detail="Le nom de la société est obligatoire")

        exists = db.query(societeleads).filter(func.lower(societeleads.nom) == nom.lower()).first()
        if exists:
            raise HTTPException(status_code=409, detail="Société déjà existante")

        obj = societeleads(nom=nom, domaine=domaine, extension=extension)
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return {"message": "Société ajoutée", "id": obj.id}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur serveur: {str(e)}")
def AddAuto(db: Session, base: str):
    """
    Ajoute automatiquement les sociétés manquantes depuis une table de leads.
    (ex: staging_leads, silver_leads, etc.)
    """
    try:
        result = db.execute(text(f"""
            WITH source_societes AS (
                SELECT DISTINCT ON (LOWER(TRIM(societe)))
                    TRIM(societe) AS nom,
                    LOWER(SPLIT_PART(SPLIT_PART(email, '@', 2), '.', 1)) AS domaine,
                    LOWER(SPLIT_PART(SPLIT_PART(email, '@', 2), '.', -1)) AS extension
                FROM {base}
                WHERE email IS NOT NULL AND email != '' AND LOWER(email) != 'nan'
                  AND societe IS NOT NULL AND societe != '' AND LOWER(societe) != 'nan'
                ORDER BY LOWER(TRIM(societe)), id
            )
            INSERT INTO societe_leads (nom, domaine, extension)
            SELECT
                src.nom,
                src.domaine,
                src.extension
            FROM source_societes src
            WHERE NOT EXISTS (
                  SELECT 1 FROM societe_leads s
                  WHERE LOWER(s.nom) = LOWER(src.nom)
              )
            ON CONFLICT (nom) DO NOTHING
        """))

        db.commit()
        added_count = result.rowcount
        return {"added_societes": int(added_count or 0)}
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def DeleteSociete(id: int, db: Session):
    try:
        societe = db.query(societeleads).filter(societeleads.id == id).first()

        if not societe:
            raise HTTPException(status_code=404, detail="Non trouvé")

        db.delete(societe)
        db.commit()
        return {"message": "Suppression réussie"}

    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Erreur serveur")

def UpdateSociete(id: int, societe_data:Societe, db: Session):
    try:
        data = societe_data.dict(exclude_unset=True)

        result = db.query(societeleads).filter(
            societeleads.id == id
        )

        if not result.first():
            raise HTTPException(status_code=404, detail="Non trouvé")

        result.update(data)
        db.commit()

        return {"message": "Modification réussie"}

    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Erreur serveur")

def GetAll(db:Session):
    return db.query(societeleads).all()

## NOTE: l'ancienne implémentation AddAuto dupliquée a été supprimée.

def get_domain(email: str):
    if not email or email.lower() in ("nan", "none", "null", ""):
        return None, None
    try:
        domain = email.split("@")[1]     
        parts = domain.split(".")
        name = parts[0]                   
        extension = parts[-1]            
        return name, extension
    except IndexError:
        return None, None