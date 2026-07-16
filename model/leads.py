from database.db import Base
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text


class Leads(Base):
    """Table unifiée (fusion de silver_leads + gold_leads).

    La complétion (part des 8 champs renseignés, 12,5% par champ) n'est PAS
    stockée : elle se calcule à partir des champs eux-mêmes — côté front pour
    l'affichage, et en SQL (voir sql_completion_expr) quand le backend doit
    distinguer Complete (100%) de Incomplete (< 100%).

    Attention : `statu` n'est PAS le niveau de qualité, c'est le statut de
    vérification de l'email ('disponible' / 'non disponible' / 'erreur').
    """

    __tablename__ = "optimized"

    id = Column(Integer, primary_key=True, autoincrement=True)

    email = Column(Text, unique=True, index=True)

    nom = Column(Text)
    prenom = Column(Text)
    fonction = Column(Text)
    societe = Column(Text)
    telephone = Column(Text)
    linkedin = Column(Text)
    location = Column(Text)

    # Statut de vérification email : 'disponible' / 'non disponible' / 'erreur'
    statu = Column(String(50))

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )
