from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text,Enum
from database.db import Base

class blacklistLeads(Base):
    __tablename__ = "blacklist_leads"

    id = Column(Integer, autoincrement=True)
    nom = Column(String(100))
    prenom = Column(String(100))
    email = Column(String(255), primary_key=True, index=True)
    fonction = Column(String(150))
    societe = Column(String(150))
    telephone = Column(String(50))
    linkedin = Column(Text)
    eliminer = Column(Enum("Unsubscribe", "archive", name="eliminer_enum"))
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )

    def __str__(self):
        return f"BlacklistLeads(id={self.id}, email={self.email})"