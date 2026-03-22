from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from schema.schemaSociete import Societe
from model.staging_leads import StagingLeads
from sqlalchemy import func

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
    societe = []

    leads = [{"email": i.email, "societe": i.societe} for i in db.query(StagingLeads).filter(
        StagingLeads.email.isnot(None),
        StagingLeads.email != "",
        func.lower(StagingLeads.email) != "nan",
        StagingLeads.societe.isnot(None),
        StagingLeads.societe != "",
        func.lower(StagingLeads.societe) != "nan",
    ).distinct(StagingLeads.societe).all()]

    existing = {i.nom.lower() for i in db.query(societeleads).all()}
    seen = set()  

    for lead in leads:
        nom = lead["societe"].lower()

        if nom in existing or nom in seen:
            continue

        seen.add(nom)
        domaine, ext = get_domain(lead["email"])

        if not domaine or not ext:
            continue

        societe.append(societeleads(
            nom=nom,
            domaine=domaine.lower(),
            extension=ext.lower()
        ))

    if societe:
        db.add_all(societe)
        db.commit()

    return {"added_societes": len(societe)}

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