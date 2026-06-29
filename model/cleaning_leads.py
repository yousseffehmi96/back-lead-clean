from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base

class cleaningleads(Base):
    __tablename__ = "cleaning_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nom = Column(Text)
    prenom = Column(Text)
    email = Column(Text)
    fonction = Column(Text)
    societe = Column(Text)
    telephone = Column(Text)
    linkedin = Column(Text)
    location = Column(Text)

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )