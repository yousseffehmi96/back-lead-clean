from pydantic import BaseModel
from typing import Optional

class Societe(BaseModel):
    nom: str
    patterne: Optional[str] = None
