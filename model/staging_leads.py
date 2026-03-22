from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base

class StagingLeads(Base):
    __tablename__ = "staging_leads"
   
    id = Column(Integer, primary_key=True, autoincrement=True)
    nom =  Column(Text, nullable=True)
    prenom =  Column(Text, nullable=True)
    email =  Column(Text, nullable=True)
    fonction = Column(Text, nullable=True)
    societe = Column(Text, nullable=True)
    telephone = Column(Text, nullable=True)
    linkedin = Column(Text, nullable=True)
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )
    def __init__(self,nom,prenom,email,fonction,societe,telephone,linkedin):
        self.nom=nom
        self.prenom=prenom
        self.email=email
        self.fonction=fonction
        self.telephone=telephone
        self.societe=societe
        self.linkedin=linkedin
    def get_nom(self):
        return self.nom

    def get_prenom(self):
        return self.prenom

    def get_email(self):
        return self.email

    def get_fonction(self):
        return self.fonction

    def get_societe(self):
        return self.societe

    def get_telephone(self):
        return self.telephone

    def get_linkedin(self):
        return self.linkedin
    def set_nom(self, nom):
        self.nom = nom
    def set_prenom(self, prenom):
        self.prenom = prenom
    def set_email(self, email):
        self.email = email
    def set_fonction(self, fonction):
        self.fonction = fonction
    def set_societe(self, societe):
        self.societe = societe
    def set_telephone(self, telephone):
        self.telephone = telephone
    def set_linkedin(self, linkedin):
        self.linkedin = linkedin
    def __str__(self):
        return f"StagingLeads(id={self.id}, nom={self.nom}, prenom={self.prenom},tel={self.telephone}, email={self.email})"