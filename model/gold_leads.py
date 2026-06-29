from database.db import Base
from sqlalchemy import Column,Integer,String,Text,TIMESTAMP,text


class Gold_leads(Base):
    __tablename__ = "gold_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)

    email = Column(Text, unique=True, index=True)

    nom = Column(Text)
    prenom = Column(Text)
    fonction = Column(Text)
    societe = Column(Text)
    telephone = Column(Text)
    linkedin = Column(Text)
    location = Column(Text)
    statu = Column(String(50))

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )