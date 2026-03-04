from sqlalchemy import Boolean, Column, DateTime, Enum, Integer, Numeric, String

from marketplace.db_models.base import BaseModel
from marketplace.models.promo_code_response import PromoCodeResponse
from marketplace.models.promo_code_type import PromoCodeType


class PromoCode(BaseModel):
    __tablename__ = 'promo_codes'

    code = Column(String(20), nullable=False)
    discount_type = Column(Enum(PromoCodeType), nullable=False)
    discount_value = Column(Numeric(12, 2), nullable=False)
    min_order_amount = Column(Numeric(12, 2), nullable=False)
    max_uses = Column(Integer, nullable=False)
    current_uses = Column(Integer, nullable=False, default=0)
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime, nullable=False)
    active = Column(Boolean, nullable=False, default=True)

    def to_pydantic(self) -> PromoCodeResponse:
        data = {
            'id': self.id,
            'code': self.code,
            'discount_type': self.discount_type,
            'discount_value': self.discount_value,
            'min_order_amount': self.min_order_amount,
            'max_uses': self.max_uses,
            'current_uses': self.current_uses,
            'valid_from': self.valid_from,
            'valid_until': self.valid_until,
            'active': self.active
        }

        return PromoCodeResponse(**data)
