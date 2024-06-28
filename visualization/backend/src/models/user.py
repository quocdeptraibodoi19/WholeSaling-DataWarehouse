from pydantic import BaseModel, Field

class UserBase(BaseModel):
    user_name: str = Field(min_length=1, max_length=50)
    first_name: str = Field(min_length=1, max_length=50)
    last_name: str = Field(min_length=1, max_length=50)

class AccountUser(UserBase):
    password: str = Field(min_length=6)

class User(UserBase):
    user_id: str

class DBUser(UserBase):
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str
