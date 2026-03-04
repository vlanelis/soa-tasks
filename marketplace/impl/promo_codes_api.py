from fastapi import HTTPException, status

from marketplace.apis.promo_codes_api_base import BasePromoCodesApi
from marketplace.database import get_session
from marketplace.db_models import PromoCode
from marketplace.models.error_response import ErrorResponse
from marketplace.models.promo_code_create_request import PromoCodeCreateRequest
from marketplace.models.promo_code_response import PromoCodeResponse
from marketplace.models.extra_models import TokenModel
from marketplace.models.user_role import UserRole


class PromoCodesApi(BasePromoCodesApi):
    async def promo_codes_post(
        self,
        promo_code_create_request: PromoCodeCreateRequest,
        token_bearer: TokenModel,
    ) -> PromoCodeResponse:
        if token_bearer.role not in (UserRole.SELLER, UserRole.ADMIN):
            error_response = ErrorResponse(
                error_code='ACCESS_DENIED',
                message='У вас недостаточно прав для выполнения этой операции',
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=error_response.model_dump(by_alias=True)
            )
        promo_code = PromoCode(
            code=promo_code_create_request.code,
            discount_type=promo_code_create_request.discount_type,
            discount_value=promo_code_create_request.discount_value,
            min_order_amount=promo_code_create_request.min_order_amount,
            max_uses=promo_code_create_request.max_uses,
            valid_from=promo_code_create_request.valid_from,
            valid_until=promo_code_create_request.valid_until,
            active=promo_code_create_request.active,
        )
        async with get_session() as session:
            session.add(promo_code)
            await session.commit()
        return promo_code.to_pydantic()
