from fastapi import UploadFile
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from model.staging_leads import StagingLeads
from model.blacklistLeads import blacklistLeads
from model.cleaning_leads import cleaningleads
from model.societe_leads import societeleads
from model.prod_leads import Prod_leads
from util.util import NetoyerUneChaine, NetoyerUnNumero, NettoyerUnEmail

def LoadFileToBd(file: UploadFile, db: Session):
    df = pd.read_csv(file.file,encoding="latin1") if ".csv" in file.filename else pd.read_excel(file.file)
    users = []
    for i in df.itertuples():
        users.append(
            StagingLeads(
                nom=None if str(i.Nom).lower() == "nan" else i.Nom,
                prenom=None if str(i.Prenom).lower() == "nan" else i.Prenom,
                email=None if str(i.Email).lower() == "nan" else i.Email,
                fonction=None if str(i.Fonction).lower() == "nan" else i.Fonction,
                societe=None if str(i.Societe).lower() == "nan" else i.Societe,
                telephone=None if str(i.Telephone).lower() == "nan" else str(i.Telephone),
                linkedin=None if str(i.Linkedin).lower() == "nan" else i.Linkedin
            )
        )
    db.add_all(users)
    db.commit()
    return {"inserted_rows": len(users)}


def SupprimerDoublons(db: Session,table:str):
    query = text(f"""
        DELETE FROM {table} a
        USING {table} b
        WHERE a.id > b.id
          AND COALESCE(a.nom, '') = COALESCE(b.nom, '')
          AND COALESCE(a.prenom, '') = COALESCE(b.prenom, '')
          AND COALESCE(a.email, '') = COALESCE(b.email, '')
          AND COALESCE(a.fonction, '') = COALESCE(b.fonction, '')
          AND COALESCE(a.societe, '') = COALESCE(b.societe, '')
          AND COALESCE(a.telephone, '') = COALESCE(b.telephone, '')
          AND COALESCE(a.linkedin, '') = COALESCE(b.linkedin, '')
    """)
    res = db.execute(query)
    db.commit()
    return {"duplicates_deleted": res.rowcount if hasattr(res, "rowcount") else 0}

def CompleteEmail(db: Session):
    result = db.query(StagingLeads).all()
    societes = db.query(societeleads).all()
    emails_completed = 0
    for user in result:
        if user.email is None and user.nom and user.prenom:
            for societe in societes:
                if user.societe and user.societe.upper() == societe.nom.upper():
                    user.email = f"{user.prenom}.{user.nom}@{societe.domaine}.{societe.extension}"
                    emails_completed += 1
                    break
    db.commit()
    return {"emails_completed": emails_completed}


def CheckContactsBlack(db: Session):
    leads = db.query(StagingLeads).all()
    blacklisted_removed = 0
    for user in leads:
        black = db.query(blacklistLeads).filter(blacklistLeads.email == user.email).first()
        if black:
            db.delete(user)
            blacklisted_removed += 1
    db.commit()
    return {"blacklisted_removed": blacklisted_removed}


def nettoyer_contact(db: Session):
    result = db.query(StagingLeads).all()
    for i in result:
        i.nom = NetoyerUneChaine(i.nom)
        i.prenom = NetoyerUneChaine(i.prenom)
        i.fonction = NetoyerUneChaine(i.fonction)
        i.societe = NetoyerUneChaine(i.societe)
        i.telephone = NetoyerUnNumero(i.telephone)
        i.email = NettoyerUnEmail(i.email)
    db.add_all(result)
    db.commit()



def StagingToProd(db: Session):
    result = db.query(StagingLeads).all()
    db_prod = []
    prod=0
    clean=0
    for row in result:
        db_prod.append(
            Prod_leads(
                nom=row.nom,
                prenom=row.prenom,
                email=row.email,
                fonction=row.fonction,
                societe=row.societe,
                telephone=row.telephone,
                linkedin=row.linkedin
            )
        )
    for row in db_prod:
        if row.email is None or row.telephone is None or row.societe is None or row.linkedin is None:
            cleanrow=cleaningleads(nom=row.nom,
                prenom=row.prenom,
                email=row.email,
                fonction=row.fonction,
                societe=row.societe,
                telephone=row.telephone,
                linkedin=row.linkedin)
            db.add(cleanrow)
            clean+=1
        else:
            print(row.nom)
            res=db.query(Prod_leads).filter(Prod_leads.email==row.email).first()
            
            if res:
                continue
            db.add(row)
            prod+=1
   
    for row in result:
        db.delete(row)
    db.commit()
    supp=SupprimerDoublons(db,"cleaning_leads")
    print(None==None)
    return {"moved_to_prod": prod,"moved to clean":clean-supp['duplicates_deleted']}