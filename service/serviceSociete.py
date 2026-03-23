from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe
from model.staging_leads import StagingLeads
from sqlalchemy import func,text
from sqlalchemy.exc import SQLAlchemyError
def AddSoc(societe:Societe,db:Session):
    result=db.query(societeleads).filter(societeleads.nom==societe.nom).first()
    if result:
        raise HTTPException(
            
            status_code=400,
            detail="La sociètè existe dèja"
        )
    try:
        db_societe = societeleads(
            nom=societe.nom,
            domaine=societe.domaine,
            extension=societe.extension
        )

    
        db.add(db_societe)
        db.commit()
        return {
            "message": "L'ajout a été effectué avec succès"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail="Erreur lors de l'ajout"
        )

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

def AddAuto(db: Session):
    try:
        
        # INSERT avec RETURNING pour obtenir le nombre exact
        result = db.execute(text("""
            WITH candidates AS (
                SELECT 
                    LOWER(TRIM(sl.societe)) as nom,
                    LOWER(SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', 1)) as domaine,
                    LOWER(SPLIT_PART(SPLIT_PART(sl.email, '@', 2), '.', 2)) as extension,
                    ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(sl.societe)) ORDER BY sl.id) as rn
                FROM staging_leads sl
                WHERE sl.email IS NOT NULL 
                  AND sl.email != ''
                  AND LOWER(sl.email) != 'nan'
                  AND sl.societe IS NOT NULL
                  AND sl.societe != ''
                  AND LOWER(sl.societe) != 'nan'
                  AND POSITION('@' IN sl.email) > 0
                  AND POSITION('.' IN SPLIT_PART(sl.email, '@', 2)) > 0
            ),
            new_societes AS (
                INSERT INTO societe_leads (nom, domaine, extension)
                SELECT c.nom, c.domaine, c.extension
                FROM candidates c
                WHERE c.rn = 1
                  AND c.domaine IS NOT NULL
                  AND c.domaine != ''
                  AND c.extension IS NOT NULL
                  AND c.extension != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM societe_leads s 
                      WHERE LOWER(s.nom) = c.nom
                  )
                RETURNING id
            )
            SELECT COUNT(*) FROM new_societes
        """))
        
        db.commit()
        
        added_count = result.scalar()
        
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