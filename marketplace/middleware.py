import json
import logging
import uuid
import time
import jwt
from typing import Optional
from datetime import datetime

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from marketplace.config import JWT_SECRET_KEY, JWT_ALGO

logger = logging.getLogger("api")
handler = logging.StreamHandler()
formatter = logging.Formatter('{"message": "%(message)s"}')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id

        start_time = time.time()
        timestamp = datetime.utcnow().isoformat()

        body = None
        if request.method in ["POST", "PUT", "DELETE"]:
            body = await self._get_request_body(request)
            if body:
                body = self._mask_sensitive_data(body)

        try:
            response = await call_next(request)
        except Exception as e:
            response = Response(status_code=500)

        duration_ms = int((time.time() - start_time) * 1000)

        user_id = self._get_user_id(request)

        log_entry = {
            "request_id": request_id,
            "method": request.method,
            "endpoint": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "user_id": user_id,
            "timestamp": timestamp
        }

        if body and request.method in ["POST", "PUT", "DELETE"]:
            log_entry["request_body"] = body

        logger.info(json.dumps(log_entry, ensure_ascii=False))

        response.headers["X-Request-Id"] = request_id

        return response

    async def _get_request_body(self, request: Request) -> Optional[dict]:
        try:
            body = await request.body()
            if body:
                return json.loads(body)
        except:
            return None
        return None

    def _mask_sensitive_data(self, data: dict) -> dict:
        sensitive_fields = ["password", "refresh_token"]

        if isinstance(data, dict):
            for key, value in data.items():
                if any(field in key.lower() for field in sensitive_fields):
                    data[key] = "***MASKED***"
                elif isinstance(value, (dict, list)):
                    self._mask_sensitive_data(value)
        elif isinstance(data, list):
            for item in data:
                self._mask_sensitive_data(item)

        return data

    def _get_user_id(self, request: Request) -> Optional[str]:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            try:
                payload = jwt.decode(auth_header.lstrip("Bearer "), JWT_SECRET_KEY, algorithms=[JWT_ALGO])
                return payload.get("user_id")
            except jwt.InvalidTokenError:
                return None
        return None
