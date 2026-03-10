from sqlalchemy.orm import Session
from model.societe_leads import societeleads
from fastapi import HTTPException
from model.prod_leads import Prod_leads
from model.blacklistLeads import blacklistLeads
from fastapi.responses import StreamingResponse

import csv
import io
def GetAllProd(db:Session):
    return db.query(Prod_leads).all()
def GetAllBlack(db:Session):
    return db.query(blacklistLeads).all()
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

