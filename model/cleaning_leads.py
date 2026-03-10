from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base

class cleaningleads(Base):
    __tablename__ = "cleaning_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nom = Column(String(100))
    prenom = Column(String(100))
    email = Column(String(255))
    fonction = Column(String(150))
    societe = Column(String(150))
    telephone = Column(String(50))
    linkedin = Column(Text)

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )