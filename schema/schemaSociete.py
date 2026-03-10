from pydantic import BaseModel
from typing import Optional

class Societe(BaseModel):
    nom: str
    domaine: str
    extension: str
