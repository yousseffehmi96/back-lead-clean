from database.db import Base
from sqlalchemy import Column,Integer,String,Text,TIMESTAMP,text


class Prod_leads(Base):
    __tablename__ = "prod_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)

    email = Column(String(255), unique=True, index=True)

    nom = Column(String(100))
    prenom = Column(String(100))
    fonction = Column(String(150))
    societe = Column(String(150))
    telephone = Column(String(50))
    linkedin = Column(Text)

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )