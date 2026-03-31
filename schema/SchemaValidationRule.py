from pydantic import BaseModel
from typing import Optional
class ValidationRuleBase(BaseModel):
    name: str
    key: str
    description: Optional[str] = None
