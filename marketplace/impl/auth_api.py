import datetime
from typing import Tuple

import bcrypt
import jwt
from fastapi import HTTPException, status
from sqlalchemy import select, exists

from marketplace.apis.auth_api_base import BaseAuthApi

from marketplace.config import JWT_SECRET_KEY, JWT_ALGO, JWT_ACCESS_TOKEN_EXPIRES, JWT_REFRESH_TOKEN_EXPIRES
from marketplace.database import get_session

from marketplace.db_models import User

from marketplace.models.error_response import ErrorResponse
from marketplace.models.error_response_details import ErrorResponseDetails
from marketplace.models.error_detail import ErrorDetail
from marketplace.models.login_request import LoginRequest
from marketplace.models.refresh_token_request import RefreshTokenRequest
from marketplace.models.register_request import RegisterRequest
from marketplace.models.token_response import TokenResponse
from marketplace.models.user_role import UserRole


def create_access_token(user_id: int, role: UserRole) -> str:
    iat = datetime.datetime.now(tz=datetime.timezone.utc)
    exp = iat + JWT_ACCESS_TOKEN_EXPIRES
    return jwt.encode({
        'user_id': user_id,
        'exp': exp,
        'type': 'access',
        'role': role
    }, JWT_SECRET_KEY, algorithm=JWT_ALGO)


def create_refresh_token(user_id: int, role: UserRole) -> str:
    iat = datetime.datetime.now(tz=datetime.timezone.utc)
    exp = iat + JWT_REFRESH_TOKEN_EXPIRES
    return jwt.encode({
        'user_id': user_id,
        'exp': exp,
        'type': 'refresh',
        'role': role
    }, JWT_SECRET_KEY, algorithm=JWT_ALGO)


def refresh_tokens(refresh_token: str) -> Tuple[str, str]:
    payload = jwt.decode(refresh_token, JWT_SECRET_KEY, algorithms=[JWT_ALGO])

    if payload.get('type') != 'refresh':
        raise ValueError("Invalid token type")

    user_id = payload.get('user_id')
    role = payload.get('role')

    if not user_id or not role:
        raise ValueError("Invalid token payload")

    new_access_token = create_access_token(user_id, role)
    new_refresh_token = create_refresh_token(user_id, role)

    return new_access_token, new_refresh_token


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


def verify_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))


class AuthApi(BaseAuthApi):
    async def auth_register_post(
            self,
            register_request: RegisterRequest,
    ) -> TokenResponse:
        user = User(
            username=register_request.username,
            hashed_password=hash_password(register_request.password),
            role=register_request.role,
        )
        async with get_session() as session:
            stmt = select(exists().where(User.username == register_request.username))
            if (await session.execute(stmt)).scalar():
                error_response = ErrorResponse(
                    error_code='VALIDATION_ERROR',
                    message='Ошибка валидации входных данных',
                    details=ErrorResponseDetails(errors=[
                        ErrorDetail(var_field="username", message="Имя пользователя уже занято")
                    ])
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=error_response.model_dump(by_alias=True)
                )
            session.add(user)
            await session.commit()
        return TokenResponse(
            access_token=create_access_token(user.id, register_request.role),
            refresh_token=create_refresh_token(user.id, register_request.role),
            expires_in=int(JWT_ACCESS_TOKEN_EXPIRES.total_seconds()),
        )

    async def auth_login_post(
            self,
            login_request: LoginRequest,
    ) -> TokenResponse:
        stmt = select(User).where(User.username == login_request.username)
        async with get_session() as session:
            result = await session.execute(stmt)
            user = result.scalar()
            if not user or not verify_password(login_request.password.get_secret_value(), user.hashed_password):
                error_response = ErrorResponse(
                    error_code='AUTHENTICATION_FAILED',
                    message='Неверные учетные данный',
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=error_response.model_dump(by_alias=True)
                )

        return TokenResponse(
            access_token=create_access_token(user.id, user.role),
            refresh_token=create_refresh_token(user.id, user.role),
            expires_in=int(JWT_ACCESS_TOKEN_EXPIRES.total_seconds()),
        )

    async def auth_refresh_post(
            self,
            refresh_token_request: RefreshTokenRequest,
    ) -> TokenResponse:
        try:
            access_token, refresh_token = refresh_tokens(refresh_token_request.refresh_token)
        except (ValueError, jwt.InvalidTokenError):
            error_response = ErrorResponse(
                error_code='REFRESH_TOKEN_INVALID',
                message="Не авторизован или токен невалиден"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=error_response.model_dump(by_alias=True)
            )
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(JWT_ACCESS_TOKEN_EXPIRES.total_seconds()),
        )
