from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from marketplace.apis.auth_api import router as AuthApiRouter
from marketplace.apis.orders_api import router as OrdersApiRouter
from marketplace.apis.products_api import router as ProductsApiRouter
from marketplace.apis.promo_codes_api import router as PromoCodesApiRouter
from marketplace.models.error_response import ErrorResponse
from marketplace.models.error_response_details import ErrorResponseDetails
from marketplace.models.error_detail import ErrorDetail
from marketplace.middleware import LoggingMiddleware

app = FastAPI(
    title="Marketplace API",
    description="Simple marketplace API",
    version="1.0.0",
)

app.include_router(AuthApiRouter)
app.include_router(OrdersApiRouter)
app.include_router(ProductsApiRouter)
app.include_router(PromoCodesApiRouter)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        loc = error.get('loc', [])
        field = '.'.join([str(x) for x in loc if x != 'body' and not isinstance(x, int)])
        errors.append(
            ErrorDetail(
                var_field=field or 'unknown',
                message=error.get('msg', 'Validation error')
            )
        )

    error_response = ErrorResponse(
        error_code="VALIDATION_ERROR",
        message="Ошибка валидации входных данных",
        details=ErrorResponseDetails(errors=errors)
    )

    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=error_response.model_dump(by_alias=True)
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.detail,
    )


app.add_middleware(LoggingMiddleware)
