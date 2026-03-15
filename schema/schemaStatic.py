from pydantic import BaseModel
from typing import Optional

class Static(BaseModel):
    inserted_rows :str
    duplicates_deleted:str
    emails_completed:str
    blacklisted_removed:str
    moved_to_prod:str
    moved_to_clean :str