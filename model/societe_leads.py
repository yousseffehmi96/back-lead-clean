from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from database.db import Base

class societeleads (Base):
     __tablename__ = "societe_leads"
     id=Column(Integer,primary_key=True,autoincrement=True)
     nom=Column(String,unique=True)
     domaine=Column(String)
     extension=Column(String)