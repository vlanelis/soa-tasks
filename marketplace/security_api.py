from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt

from marketplace.config import JWT_SECRET_KEY, JWT_ALGO
from marketplace.models.error_response import ErrorResponse
from marketplace.models.error_response_details import ErrorResponseDetails
from marketplace.models.error_detail import ErrorDetail
from marketplace.models.extra_models import TokenModel


bearer_auth = HTTPBearer()


def get_token_bearer(credentials: HTTPAuthorizationCredentials = Depends(bearer_auth)) -> TokenModel:
    token = credentials.credentials
    error_response = None
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGO])
        return TokenModel(user_id=payload["user_id"], role=payload["role"])
    except jwt.ExpiredSignatureError:
        error_response = ErrorResponse(
            error_code='TOKEN_EXPIRED',
            message='Срок действия токена истек',
        )
    except jwt.InvalidTokenError:
        error_response = ErrorResponse(
            error_code='TOKEN_INVALID',
            message='Недействительный токен',
        )
    finally:
        if error_response:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=error_response.model_dump(by_alias=True)
            )



