from sqlalchemy.orm import Session
from model.validationRule import ValidationRule
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError
from schema.SchemaValidationRule import ValidationRuleBase

# 📥 GET ALL
def GetAllRules(db: Session):
    try:
        return db.query(ValidationRule).all()

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


# ➕ ADD
def AddRule(rule:ValidationRuleBase, db: Session):
    try:
        # Vérifier doublon key
        existing = db.query(ValidationRule).filter(ValidationRule.key == rule.key).first()
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Cette clé existe déjà"
            )

        new_rule = ValidationRule(
            name=rule.name,
            key=rule.key,
            description=rule.description,
        )

        db.add(new_rule)
        db.commit()
        db.refresh(new_rule)

        return new_rule

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")


# ✏️ UPDATE
def UpdateRule(id: int, data, db: Session):
    try:
        rule = db.query(ValidationRule).filter(ValidationRule.id == id).first()

        if not rule:
            raise HTTPException(
                status_code=404,
                detail="Règle non trouvée"
            )

        # Vérifier doublon key (si modifiée)
        existing = db.query(ValidationRule).filter(
            ValidationRule.key == data.key,
            ValidationRule.id != id
        ).first()

        if existing:
            raise HTTPException(
                status_code=400,
                detail="Cette clé existe déjà"
            )

        rule.name = data.name
        rule.key = data.key
        rule.description = data.description

        db.commit()
        db.refresh(rule)

        return rule

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")



def DeleteRule(id: int, db: Session):
    try:
        rule = db.query(ValidationRule).filter(ValidationRule.id == id).first()

        if not rule:
            raise HTTPException(
                status_code=404,
                detail="Règle non trouvée"
            )

        db.delete(rule)
        db.commit()

        return {
            "message": "Règle supprimée avec succès"
        }

    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur base de données : {str(e)}")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur inattendue : {str(e)}")