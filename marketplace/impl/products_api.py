from typing import Optional

from fastapi import HTTPException, Response, status
from typing_extensions import Annotated

from pydantic import Field, StrictInt, StrictStr
from sqlalchemy import select
from sqlalchemy.sql import func

from marketplace.apis.products_api_base import BaseProductsApi
from marketplace.database import get_session
from marketplace.db_models import Product

from marketplace.models.error_response import ErrorResponse
from marketplace.models.extra_models import TokenModel
from marketplace.models.products_get200_response import ProductsGet200Response
from marketplace.models.product_create import ProductCreate
from marketplace.models.product_response import ProductResponse
from marketplace.models.product_status import ProductStatus
from marketplace.models.product_update import ProductUpdate
from marketplace.models.user_role import UserRole


class ProductsApi(BaseProductsApi):
    async def products_get(
        self,
        page: Annotated[
            Optional[Annotated[int, Field(strict=True, ge=0)]], Field(description="Номер страницы (начиная с 0)")],
        size: Annotated[
            Optional[Annotated[int, Field(le=100, strict=True, ge=1)]], Field(description="Размер страницы")],
        status: Annotated[Optional[ProductStatus], Field(description="Фильтр по статусу")],
        category: Annotated[Optional[StrictStr], Field(description="Фильтр по категории (точное совпадение)")],
        token_bearer: TokenModel,
    ) -> ProductsGet200Response:
        current_page = page if page is not None else 0
        page_size = size if size is not None else 20

        async with get_session() as session:
            query = select(Product)

            if status is not None:
                query = query.where(Product.status == status)
            if category is not None:
                query = query.where(Product.category == category)

            paginated_query = (
                query
                .offset(current_page * page_size)
                .limit(page_size)
            )

            result = await session.execute(paginated_query)
            products = result.scalars().all()

            count_query = select(func.count()).select_from(query.subquery())
            total_elements = await session.scalar(count_query)

            content = [product.to_pydantic() for product in products]
            total_pages = (total_elements + page_size - 1) // page_size

            return ProductsGet200Response(
                content=content,
                total_elements=total_elements,
                current_page=current_page,
                page_size=page_size,
                total_pages=total_pages
            )

    async def products_post(
            self,
            product_create: ProductCreate,
            token_bearer: TokenModel,
    ) -> ProductResponse:
        if token_bearer.role == UserRole.SELLER:
            seller_id = token_bearer.user_id
        elif token_bearer.role == UserRole.ADMIN:
            seller_id = product_create.seller_id if product_create.seller_id is not None else token_bearer.user_id
        else:
            error_response = ErrorResponse(
                error_code='ACCESS_DENIED',
                message='У вас недостаточно прав для выполнения этой операции',
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=error_response.model_dump(by_alias=True)
            )
        product = Product(
            name=product_create.name,
            description=product_create.description,
            price=product_create.price,
            stock=product_create.stock,
            category=product_create.category,
            status=product_create.status,
            seller_id=seller_id,
        )
        async with get_session() as session:
            session.add(product)
            await session.commit()
        return product.to_pydantic()

    async def products_id_get(
        self,
        id: StrictInt,
        token_bearer: TokenModel,
    ) -> ProductResponse:
        async with get_session() as session:
            product = await session.get(Product, id)
            if not product:
                error_response = ErrorResponse(
                    error_code='PRODUCT_NOT_FOUND',
                    message="Продукт не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )
            return product.to_pydantic()

    async def products_id_put(
        self,
        id: StrictInt,
        product_update: ProductUpdate,
        token_bearer: TokenModel,
    ) -> ProductResponse:
        async with get_session() as session:
            product = await session.get(Product, id)
            if not product:
                error_response = ErrorResponse(
                    error_code='PRODUCT_NOT_FOUND',
                    message="Продукт не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )
            if token_bearer.role == UserRole.SELLER and product.seller_id == token_bearer.user_id:
                seller_id = token_bearer.user_id
            elif token_bearer.role == UserRole.ADMIN:
                seller_id = product_update.seller_id if product_update.seller_id is not None else token_bearer.user_id
            else:
                error_response = ErrorResponse(
                    error_code='ACCESS_DENIED',
                    message='У вас недостаточно прав для выполнения этой операции',
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=error_response.model_dump(by_alias=True)
                )

            product.name = product_update.name
            product.description = product_update.description
            product.price = product_update.price
            product.stock = product_update.stock
            product.category = product_update.category
            product.status = product_update.status
            product.seller_id = seller_id

            await session.commit()
            await session.refresh(product)
            return product.to_pydantic()

    async def products_id_delete(
        self,
        id: StrictInt,
        token_bearer: TokenModel,
    ) -> None:
        async with get_session() as session:
            product = await session.get(Product, id)
            if not product:
                error_response = ErrorResponse(
                    error_code='PRODUCT_NOT_FOUND',
                    message="Продукт не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )
            if token_bearer.user_id != product.seller_id and token_bearer.role != UserRole.ADMIN:
                error_response = ErrorResponse(
                    error_code='ACCESS_DENIED',
                    message='У вас недостаточно прав для выполнения этой операции',
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=error_response.model_dump(by_alias=True)
                )

            product.status = ProductStatus.ARCHIVED
            await session.commit()

            return Response(status_code=204)
