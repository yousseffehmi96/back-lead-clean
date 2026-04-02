from sqlalchemy import *
from database.db import Base

class Token(Base):
    __tablename__ = "token"
    id=Column(INTEGER, primary_key=True, autoincrement=True)
    name=Column(Text)
    token=Column(Text)
    created_at = Column(
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP")
    )
