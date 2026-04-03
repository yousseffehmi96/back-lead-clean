from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base

class StatisticLeads(Base):
    __tablename__ = "statistic_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String)

    inserted_rows = Column(Integer)

    emails_completed = Column(Integer)
    societe_completed = Column(Integer)  
    added_societes = Column(Integer)      

    blacklisted_removed = Column(Integer)

    moved_to_silver = Column(Integer)
    moved_to_clean = Column(Integer)
    moved_to_gold = Column(Integer)

    staging_vs_silver = Column(Integer)
    staging_vs_gold = Column(Integer)
    staging_internal = Column(Integer)

    total_deleted = Column(Integer)
    iduser=Column(Text)       

    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )