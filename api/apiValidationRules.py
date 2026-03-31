from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import service.serviceValidationRule as Svr
from database.db import get_db
from schema.SchemaValidationRule import ValidationRuleBase

routes = APIRouter()

# ➕ CREATE
@routes.post("/validation-rules")
async def add_rule(rule: ValidationRuleBase, db: Session = Depends(get_db)):
    print("rule",rule)
    return Svr.AddRule(rule, db)

# ❌ DELETE
@routes.delete("/validation-rules/{id}")
async def delete_rule(id: int, db: Session = Depends(get_db)):
    return Svr.DeleteRule(id, db)

# ✏️ UPDATE
@routes.put("/validation-rules/{id}")
async def update_rule(id: int, rule: ValidationRuleBase, db: Session = Depends(get_db)):
    return Svr.UpdateRule(id, rule, db)

# 📥 GET ALL
@routes.get("/validation-rules")
async def get_all_rules(db: Session = Depends(get_db)):
    return Svr.GetAllRules(db)