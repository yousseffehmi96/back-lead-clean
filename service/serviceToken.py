from sqlalchemy.orm import Session
from model.token import Token
from schema.schemaToken import SchemaToken
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError
import secrets
import hashlib
from datetime import datetime




def GetAllToken(db:Session):
    try:
        tokens=db.query(Token).all()
        return tokens
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code="500",detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code="500",detail=f"Erreur inattendue : {str(e)}")

def AddToken(token:SchemaToken,db:Session):
    try:
      tokenToAdd=Token(
        name=token.name,
        token=token.token,
        created_at=datetime.utcnow()

      )
      db.add(tokenToAdd)
      db.commit()
      return {
            "message": "Token ajoutè avec succès"
        }
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code="500",detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code="500",detail=f"Erreur inattendue : {str(e)}")

def delete(db:Session,id:int):
    try:
        token=db.query(Token).filter(Token.id==id).first()
        db.delete(token)
        db.commit()
        return {
            "message": "Token supprimé avec succès"
        }
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code="500",detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code="500",detail=f"Erreur inattendue : {str(e)}")
        