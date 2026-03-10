from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe


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