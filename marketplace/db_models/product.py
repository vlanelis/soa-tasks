from sqlalchemy import BigInteger, Column, DateTime, Enum, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.sql import func

from marketplace.db_models.base import BaseModel
from marketplace.models.product_response import ProductResponse
from marketplace.models.product_status import ProductStatus


class Product(BaseModel):
    __tablename__ = 'products'

    name = Column(String(255), nullable=False)
    description = Column(String(4000), nullable=True)
    price = Column(Numeric(12, 2), nullable=False)
    stock = Column(Integer, nullable=False)
    category = Column(String(100), nullable=False)
    status = Column(Enum(ProductStatus), nullable=False)

    seller_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_products_status', 'status'),
    )

    def to_pydantic(self) -> ProductResponse:
        data = {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'price': self.price,
            'stock': self.stock,
            'category': self.category,
            'status': self.status,
            'seller_id': self.seller_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

        return ProductResponse(**data)
