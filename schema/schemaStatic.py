from pydantic import BaseModel
from typing import Optional

class Static(BaseModel):
    filename: str
    inserted_rows: int
    duplicates_deleted: int
    emails_completed: int
    blacklisted_removed: int
    moved_to_silver: int
    moved_to_clean: int
    moved_to_gold: int