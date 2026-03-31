from sqlalchemy import Column, Integer, String, Boolean, Text
from database.db import Base

class ValidationRule(Base):
    __tablename__ = "validation_rules"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    key = Column(String, unique=True, nullable=False)
    description = Column(Text, nullable=True)
