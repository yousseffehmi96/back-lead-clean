from fastapi import APIRouter,Depends
from database.db import get_db
from schema.schemaToken import SchemaToken
import service.serviceToken as st
from sqlalchemy.orm import Session

Route=APIRouter()


@Route.get("/token")
def get_all_tokens(db:Session=Depends(get_db)):
    return st.GetAllToken(db)
@Route.post("/token")
def Add_Token(token:SchemaToken,db:Session=Depends(get_db)):
    return st.AddToken(token,db)
@Route.delete("/token/{id}")
def Delete_Token(id:int,db:Session=Depends(get_db)):
    return st.delete(db,id)