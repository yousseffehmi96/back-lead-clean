from sqlalchemy import Column, Integer, Text, TIMESTAMP, text
from database.db import Base


class StagingImportHistory(Base):
    __tablename__ = "staging_import_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(Text, nullable=True)
    iduser = Column(Text, nullable=True)
    nom = Column(Text, nullable=True)
    prenom = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    fonction = Column(Text, nullable=True)
    societe = Column(Text, nullable=True)
    telephone = Column(Text, nullable=True)
    linkedin = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    imported_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )
