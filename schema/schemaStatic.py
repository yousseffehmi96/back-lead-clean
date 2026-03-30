from pydantic import BaseModel
from typing import Optional

class Static(BaseModel):
    filename: str
    inserted_rows: int
    emails_completed: int
    blacklisted_removed: int
    moved_to_silver: int
    moved_to_clean: int
    moved_to_gold: int
    staging_vs_silver:int
    staging_vs_gold:int
    staging_internal:int    