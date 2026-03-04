from sqlalchemy import BigInteger, Column, ForeignKey, Integer, Numeric
from sqlalchemy.orm import relationship

from marketplace.db_models.base import BaseModel
from marketplace.models.order_item_response import OrderItemResponse


class OrderItem(BaseModel):
    __tablename__ = 'order_items'

    order_id = Column(BigInteger, ForeignKey('orders.id', ondelete='CASCADE'), nullable=False)
    product_id = Column(BigInteger, ForeignKey('products.id', ondelete='CASCADE'), nullable=False)
    quantity = Column(Integer, nullable=False)
    price_at_order = Column(Numeric(12, 2), nullable=False)

    product = relationship("Product", backref="order_items")

    def to_pydantic(self) -> OrderItemResponse:
        data = {
            'product_id': self.product_id,
            'quantity': self.quantity,
            'price_at_order': self.price_at_order,
        }

        return OrderItemResponse(**data)
