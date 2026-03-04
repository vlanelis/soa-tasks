import enum

from sqlalchemy import BigInteger, Column, DateTime, Enum, ForeignKey
from sqlalchemy.sql import func

from marketplace.db_models.base import BaseModel


class UserOperationType(enum.Enum):
    CREATE_ORDER = "CREATE_ORDER"
    UPDATE_ORDER = "UPDATE_ORDER"


class UserOperation(BaseModel):
    __tablename__ = 'user_operations'

    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    operation_type = Column(Enum(UserOperationType), nullable=False)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
