from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base


class StatisticLeads(Base):
    __tablename__ = "statistic_leads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    inserted_rows = Column(Integer)
    duplicates_deleted=Column(Integer)
    emails_completed=Column(Integer)
    blacklisted_removed=Column(Integer)
    moved_to_prod =Column(Integer)
    moved_to_clean =Column(Integer)
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )