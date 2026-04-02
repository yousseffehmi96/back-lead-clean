from pydantic import *
class SchemaToken(BaseModel):
    name:str
    token:str