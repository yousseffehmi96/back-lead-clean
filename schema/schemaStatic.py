from pydantic import BaseModel
from typing import Optional

class Static(BaseModel):
    filename: str
    inserted_rows: int
    emails_completed: int
    blacklisted_removed: int
    moved_to_incomplete: int
    moved_to_clean: int
    moved_to_complete: int
    staging_vs_incomplete:int
    staging_vs_complete:int
    staging_internal:int    
    iduser:str