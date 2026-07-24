from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base
from dotenv import load_dotenv
import os
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
print(DATABASE_URL)
#Ouvre la connection avec postgres
# pool_pre_ping : teste (et rétablit) une connexion périmée avant chaque usage.
#   -> corrige "SSL SYSCALL error: EOF detected" quand Postgres coupe une connexion inactive.
# pool_recycle : recycle les connexions plus vieilles que 30 min (avant le timeout serveur).
# keepalives : maintient la connexion SSL active côté TCP (Postgres managé / Supabase / Neon).
engine = create_engine(
    DATABASE_URL,
    pool_timeout=5,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=10,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)
SessionLocal=sessionmaker(bind=engine)
Base=declarative_base()
def get_db():
    db=SessionLocal()
    try:
        print(db)
        yield db
    finally:
        db.close()

