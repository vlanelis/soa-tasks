from sqlalchemy import BigInteger, Column, DateTime, Enum, ForeignKey, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from marketplace.db_models.base import BaseModel
from marketplace.models.order_response import OrderResponse
from marketplace.models.order_status import OrderStatus


class Order(BaseModel):
    __tablename__ = 'orders'

    user_id = Column(BigInteger, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    status = Column(Enum(OrderStatus), nullable=False)
    promo_code_id = Column(BigInteger, ForeignKey('promo_codes.id', ondelete='SET NULL'), nullable=True)
    total_amount = Column(Numeric(12, 2), nullable=False)
    discount_amount = Column(Numeric(12, 2), nullable=False)

    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    items = relationship('OrderItem', backref='order', cascade="all, delete-orphan", lazy='selectin')

    def to_pydantic(self) -> OrderResponse:
        data = {
            'id': self.id,
            'user_id': self.user_id,
            'status': self.status,
            'total_amount': self.total_amount,
            'discount_amount': self.discount_amount,
            'promo_code_id': self.promo_code_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'items': [item.to_pydantic() for item in self.items]
        }

        return OrderResponse(**data)
