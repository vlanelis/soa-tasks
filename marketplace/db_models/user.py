from sqlalchemy import Column, Enum, String

from marketplace.db_models.base import BaseModel
from marketplace.models.user_role import UserRole


class User(BaseModel):
    __tablename__ = 'users'

    username = Column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    role = Column(Enum(UserRole), nullable=False)
