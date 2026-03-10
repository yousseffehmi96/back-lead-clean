from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base
from dotenv import load_dotenv
import os
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)
#Ouvre la connection avec postgres
engine=create_engine(DATABASE_URL)
SessionLocal=sessionmaker(bind=engine)
Base=declarative_base()
def get_db():
    db=SessionLocal()
    try:
        print(db)
        yield db
    finally:
        db.close()

