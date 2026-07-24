from database.db import Base
from sqlalchemy import Column, Integer, Text, TIMESTAMP, text


class ExportLeads(Base):
    """Instantané du résultat d'un import, juste après nettoyage.

    Alimentée à la fin de « Importer & nettoyer », avant que les leads ne
    partent vers staging. Sert de base au fichier Excel remis à l'utilisateur :
    on garde ainsi une trace exacte de ce qui a été livré pour cet import,
    même si les leads évoluent ensuite dans le pipeline.
    """

    __tablename__ = "export_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)

    nom = Column(Text, nullable=True)
    prenom = Column(Text, nullable=True)
    email = Column(Text, nullable=True)
    fonction = Column(Text, nullable=True)
    societe = Column(Text, nullable=True)
    telephone = Column(Text, nullable=True)
    linkedin = Column(Text, nullable=True)
    location = Column(Text, nullable=True)

    # Rattachement à l'import d'origine (pour n'exporter que le dernier lot)
    filename = Column(Text, nullable=True, index=True)
    iduser = Column(Text, nullable=True, index=True)

    exported_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )
