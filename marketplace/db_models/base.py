from sqlalchemy import Column, BigInteger
from marketplace.database import Base


class BaseModel(Base):
    __abstract__ = True

    id = Column(BigInteger, primary_key=True, index=True)
