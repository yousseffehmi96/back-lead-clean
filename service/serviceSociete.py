from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe
from model.staging_leads import StagingLeads
from sqlalchemy import func,text
from sqlalchemy.exc import SQLAlchemyError
def AddAuto(db: Session):
    try:
        # Debug: voir ce que staging contient
        debug = db.execute(text("""
            SELECT 
                societe,
                email,
                LOWER(TRIM(societe)) as nom_calc,
                LOWER(REGEXP_REPLACE(SPLIT_PART(email, '@', 2), '\\.[^.]+$', '')) as domaine_calc,
                LOWER(REGEXP_REPLACE(SPLIT_PART(email, '@', 2), '^.*\\.', '')) as extension_calc
            FROM staging_leads
            WHERE email IS NOT NULL AND email != '' AND LOWER(email) != 'nan'
              AND societe IS NOT NULL AND societe != '' AND LOWER(societe) != 'nan'
            LIMIT 5
        """))
        print("=== DEBUG staging_leads ===")
        for row in debug:
            print(f"  societe='{row.societe}' | nom='{row.nom_calc}' | domaine='{row.domaine_calc}' | ext='{row.extension_calc}'")

        # Debug: voir ce qui est bloqué par NOT EXISTS
        blocked = db.execute(text("""
            SELECT LOWER(TRIM(sl.societe)) as nom, s.nom as bloque_par
            FROM staging_leads sl
            JOIN societe_leads s ON LOWER(s.nom) = LOWER(TRIM(sl.societe))
            LIMIT 5
        """))
        print("=== Bloqués par NOT EXISTS ===")
        for row in blocked:
            print(f"  '{row.nom}' bloqué par '{row.bloque_par}'")

        result = db.execute(text("""..."""))  # votre requête normale
        db.commit()
        added_count = result.scalar()
        print(f"✅ {added_count} nouvelles sociétés ajoutées")
    finally:
        pass
        return {"added_societes": added_count}


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

def AddAuto(db: Session, base: str):
    try:
        # Insérer les sociétés uniques à partir des emails
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
        
        added_count = result.rowcount  # nombre de lignes insérées
        print(f"✅ {added_count} nouvelles sociétés ajoutées")
        return {"added_societes": added_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

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