from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from model.silver_leads import Silver_leads
from model.gold_leads import Gold_leads
from model.blacklistLeads import blacklistLeads
from fastapi.responses import StreamingResponse
from model.cleaning_leads import cleaningleads
from model.statistiqueLeads import StatisticLeads
from model.staging_leads import StagingLeads
from model.gold_leads import Gold_leads
from sqlalchemy import or_,and_
from sqlalchemy import text
import csv
import io
from sqlalchemy.exc import SQLAlchemyError
def GetAllSilver(db:Session):
    return db.query(Silver_leads).all()
def GetAllGold(db:Session):
    return db.query(Gold_leads).all()
def GetAllBlack(db:Session):
    return db.query(blacklistLeads).all()
def GetAllClean(db:Session):
    return db.query(cleaningleads).all()
def GetAllStat(db:Session):
    return db.query(StatisticLeads).all()
def GetAllStaging(db:Session):
    return db.query(StagingLeads).all()
def DowloadProdLead(db:Session):
    leads=db.query(Silver_leads).all()
    output=io.StringIO()
    write=csv.writer(output)
    write.writerow([
        "Nom",
        "Prenom",
        "Email",
        "Fonction",
        "Societe",
        "Telephone",
        "Linkedin"
    ])
    for lead in leads:
        write.writerow([
            lead.nom,
            lead.prenom,
            lead.email,
            lead.fonction,
            lead.societe,
            lead.telephone,
            lead.linkedin
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=leads.csv"}
    )

def ToBlack(id:int,eliminer:str,db:Session):
    result =db.query(Silver_leads).filter(Silver_leads.id==id).first()
    if (not result):
        raise HTTPException(
               status_code=404,
               detail='Leads non trouvè'
        )
    print(result.nom)
    blocklead=blacklistLeads(
                    id=result.id,
                    nom=result.nom,
                    prenom= result.prenom,
                    email=result.email,
                    fonction= result.fonction,
                    societe= result.societe,
                    telephone=result.telephone,
                    linkedin= result.linkedin,
                    eliminer=eliminer
                )
    print(blocklead)
    db.add(blocklead)
    db.delete(result)
    db.commit()
    return {
            "message": "Le leads a èté blocque avec succeè"
        }
    
def StagingToSilver(db: Session):
    try:
        # 1️⃣ INSERT INTO silver_leads depuis staging (évite les doublons)
        result = db.execute(text("""
            INSERT INTO silver_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin
            FROM staging_leads
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND (
                  fonction IS NULL OR fonction = '' OR fonction = 'nan'
                  OR telephone IS NULL OR telephone = '' OR telephone = 'nan'
                  OR linkedin IS NULL OR linkedin = '' OR linkedin = 'nan'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM silver_leads s WHERE s.email = staging_leads.email
              )
            ORDER BY email, id
        """))
        
        moved_count = result.rowcount
        
        # 2️⃣ DELETE depuis staging (ceux qui ont été déplacés + doublons internes)
        db.execute(text("""
            DELETE FROM staging_leads
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND (
                  fonction IS NULL OR fonction = '' OR fonction = 'nan'
                  OR telephone IS NULL OR telephone = '' OR telephone = 'nan'
                  OR linkedin IS NULL OR linkedin = '' OR linkedin = 'nan'
              )
        """))
        
        db.commit()
        
        print(f"✅ {moved_count} leads déplacés vers Silver")
        return {"moved_to_silver": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")
def StagingToClean(db: Session):
    try:
        
        # 1️⃣ INSERT INTO cleaning_leads depuis staging (évite les doublons)
        result = db.execute(text("""
            INSERT INTO cleaning_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (COALESCE(email, id::text)) 
                nom, prenom, email, fonction, societe, telephone, linkedin
            FROM staging_leads sl
            WHERE email IS NULL 
               OR NOT EXISTS (
                   SELECT 1 FROM cleaning_leads cl WHERE cl.email = sl.email
               )
            ORDER BY COALESCE(email, id::text), id
        """))
        
        moved_count = result.rowcount
        
        # 2️⃣ DELETE tous les leads de staging
        db.execute(text("""
            DELETE FROM staging_leads
        """))
        
        db.commit()
        
        print(f"✅ {moved_count} leads déplacés vers Cleaning")
        return {"moved_to_clean": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")

def StagingToGold(db: Session):
    try:
        
        # 1️⃣ INSERT INTO gold_leads depuis staging (évite les doublons)
        result = db.execute(text("""
            INSERT INTO gold_leads (nom, prenom, email, fonction, societe, telephone, linkedin)
            SELECT DISTINCT ON (email) 
                nom, prenom, email, fonction, societe, telephone, linkedin
            FROM staging_leads
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND fonction IS NOT NULL 
              AND fonction != '' 
              AND fonction != 'nan'
              AND telephone IS NOT NULL 
              AND telephone != '' 
              AND telephone != 'nan'
              AND linkedin IS NOT NULL 
              AND linkedin != '' 
              AND linkedin != 'nan'
              AND NOT EXISTS (
                  SELECT 1 FROM gold_leads g WHERE g.email = staging_leads.email
              )
            ORDER BY email, id
        """))
        
        moved_count = result.rowcount
        
        # 2️⃣ DELETE depuis staging (ceux qui ont été déplacés + doublons internes)
        db.execute(text("""
            DELETE FROM staging_leads
            WHERE email IS NOT NULL 
              AND email != '' 
              AND email != 'nan'
              AND nom IS NOT NULL 
              AND nom != '' 
              AND nom != 'nan'
              AND prenom IS NOT NULL 
              AND prenom != '' 
              AND prenom != 'nan'
              AND societe IS NOT NULL 
              AND societe != '' 
              AND societe != 'nan'
              AND fonction IS NOT NULL 
              AND fonction != '' 
              AND fonction != 'nan'
              AND telephone IS NOT NULL 
              AND telephone != '' 
              AND telephone != 'nan'
              AND linkedin IS NOT NULL 
              AND linkedin != '' 
              AND linkedin != 'nan'
        """))
        
        db.commit()
        
        print(f"✅ {moved_count} leads déplacés vers Gold")
        return {"moved_to_gold": moved_count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


def CompleteSocieteFromEmail(db: Session):
    try:        
        # UPDATE avec extraction du domaine en SQL pur
        result = db.execute(text("""
            UPDATE staging_leads
            SET societe = INITCAP(
                REPLACE(
                    SPLIT_PART(SPLIT_PART(email, '@', 2), '.', 1),
                    '-',
                    ' '
                )
            )
            WHERE email IS NOT NULL 
              AND email != ''
              AND email LIKE '%@%.%'
              AND (societe IS NULL OR societe = '')
        """))
        
        db.commit()
        count = result.rowcount
        
        print(f"✅ {count} sociétés complétées depuis les emails")
        return {"societe_completed": count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")



def CompleteNomPrenomFromEmail(db: Session):
    try:
        
        # UPDATE avec extraction du nom et prénom en SQL pur (PostgreSQL)
        result = db.execute(text("""
            UPDATE staging_leads
            SET 
                prenom = CASE 
                    WHEN (prenom IS NULL OR prenom = '') 
                         AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
                    THEN INITCAP(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 1))
                    ELSE prenom
                END,
                nom = CASE 
                    WHEN (nom IS NULL OR nom = '') 
                         AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
                    THEN INITCAP(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 2))
                    ELSE nom
                END
            WHERE email IS NOT NULL 
              AND email != ''
              AND email LIKE '%@%'
              AND POSITION('.' IN SPLIT_PART(email, '@', 1)) > 0
              AND (
                  (prenom IS NULL OR prenom = '') 
                  OR (nom IS NULL OR nom = '')
              )
        """))
        
        db.commit()
        count = result.rowcount
        
        print(f"✅ {count} noms/prénoms complétés depuis les emails")
        return {"nom_prenom_completed": count}

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")