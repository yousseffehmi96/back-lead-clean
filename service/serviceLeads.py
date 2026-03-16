from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from model.prod_leads import Prod_leads
from model.blacklistLeads import blacklistLeads
from fastapi.responses import StreamingResponse
from model.cleaning_leads import cleaningleads
from model.statistiqueLeads import StatisticLeads
import csv
import io
def GetAllProd(db:Session):
    return db.query(Prod_leads).all()
def GetAllBlack(db:Session):
    return db.query(blacklistLeads).all()
def GetAllClean(db:Session):
    return db.query(cleaningleads).all()
def GetAllStat(db:Session):
    return db.query(StatisticLeads).all()
def DowloadProdLead(db:Session):
    leads=db.query(Prod_leads).all()
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
    result =db.query(Prod_leads).filter(Prod_leads.id==id).first()
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
    

