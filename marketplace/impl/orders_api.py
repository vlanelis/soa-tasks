import datetime
from decimal import Decimal

from fastapi import HTTPException, Response, status

from pydantic import StrictInt
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from marketplace.apis.orders_api_base import BaseOrdersApi
from marketplace.config import ORDER_CREATE_COOLDOWN, ORDER_UPDATE_COOLDOWN
from marketplace.database import get_session
from marketplace.db_models import Product, Order, OrderItem, PromoCode, UserOperation, UserOperationType
from marketplace.models.error_response import ErrorResponse
from marketplace.models.error_response_details import ErrorResponseDetails
from marketplace.models.order_create_request import OrderCreateRequest
from marketplace.models.order_response import OrderResponse
from marketplace.models.order_status import OrderStatus
from marketplace.models.order_update_request import OrderUpdateRequest
from marketplace.models.product_status import ProductStatus
from marketplace.models.promo_code_type import PromoCodeType
from marketplace.models.stock_issue import StockIssue
from marketplace.models.extra_models import TokenModel
from marketplace.models.user_role import UserRole


class OrdersApi(BaseOrdersApi):
    async def orders_post(
        self,
        order_create_request: OrderCreateRequest,
        token_bearer: TokenModel,
    ) -> OrderResponse:
        if token_bearer.role == UserRole.SELLER:
            error_response = ErrorResponse(
                error_code="ACCESS_DENIED",
                message="У вас недостаточно прав для выполнения этой операции"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=error_response.model_dump(by_alias=True)
            )

        user_id = token_bearer.user_id
        now = datetime.datetime.now()

        async with get_session() as session:
            last_op_stmt = (
                select(UserOperation)
                .where(
                    UserOperation.user_id == user_id,
                    UserOperation.operation_type == UserOperationType.CREATE_ORDER
                )
                .order_by(UserOperation.created_at.desc())
                .limit(1)
            )
            last_op = await session.scalar(last_op_stmt)
            if last_op:
                if last_op.created_at > now - ORDER_CREATE_COOLDOWN:
                    error_response = ErrorResponse(
                        error_code="ORDER_LIMIT_EXCEEDED",
                        message="Слишком частые попытки создания заказа"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=error_response.model_dump(by_alias=True)
                    )

            active_stmt = (
                select(Order)
                .where(
                    Order.user_id == user_id,
                    Order.status.in_([OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING])
                )
                .with_for_update()
            )
            active_order = await session.scalar(active_stmt)
            if active_order:
                error_response = ErrorResponse(
                    error_code="ORDER_HAS_ACTIVE",
                    message="У вас уже есть активный заказ"
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            product_ids = [item.product_id for item in order_create_request.items]
            products_stmt = (
                select(Product)
                .where(Product.id.in_(product_ids))
                .with_for_update()
            )
            result = await session.execute(products_stmt)
            products = {p.id: p for p in result.scalars().all()}

            missing_ids = set(product_ids) - set(products.keys())
            if missing_ids:
                error_response = ErrorResponse(
                    error_code="PRODUCT_NOT_FOUND",
                    message="Некоторые товары не найдены",
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )

            inactive_products = [p.id for p in products.values() if p.status != ProductStatus.ACTIVE]
            if inactive_products:
                error_response = ErrorResponse(
                    error_code="PRODUCT_INACTIVE",
                    message="Некоторые товары неактивны",
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            insufficient_stock = []
            for item in order_create_request.items:
                product = products[item.product_id]
                if product.stock < item.quantity:
                    insufficient_stock.append(StockIssue(
                        product_id=item.product_id,
                        requested_quantity=item.quantity,
                        available_stock=product.stock
                    ))

            if insufficient_stock:
                error_response = ErrorResponse(
                    error_code="INSUFFICIENT_STOCK",
                    message="Недостаточно товара на складе",
                    details=ErrorResponseDetails(stock_issues=insufficient_stock)
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            subtotal = Decimal(0)
            for item in order_create_request.items:
                product = products[item.product_id]
                subtotal += Decimal(product.price) * Decimal(item.quantity)

            promo_code_obj = None
            discount_amount = Decimal(0)
            total_amount = subtotal

            if order_create_request.promo_code:
                promo_stmt = (
                    select(PromoCode)
                    .where(PromoCode.code == order_create_request.promo_code)
                    .with_for_update()
                )
                promo_code_obj = await session.scalar(promo_stmt)
                if not promo_code_obj:
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_INVALID",
                        message="Промокод не найден"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if not promo_code_obj.active:
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_INVALID",
                        message="Промокод неактивен"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if promo_code_obj.current_uses >= promo_code_obj.max_uses:
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_INVALID",
                        message="Исчерпан лимит использования промокода"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if not (promo_code_obj.valid_from <= now <= promo_code_obj.valid_until):
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_INVALID",
                        message="Срок действия промокода истёк"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if subtotal < promo_code_obj.min_order_amount:
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_MIN_AMOUNT",
                        message="Сумма заказа меньше минимальной для применения промокода"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if promo_code_obj.discount_type == PromoCodeType.PERCENTAGE:
                    discount = subtotal * Decimal(promo_code_obj.discount_value) / Decimal(100)
                    max_discount = subtotal * Decimal("0.7")
                    discount_amount = min(discount, max_discount)
                else:
                    discount_amount = min(Decimal(promo_code_obj.discount_value), subtotal)

                total_amount = subtotal - discount_amount

            for item in order_create_request.items:
                product = products[item.product_id]
                product.stock -= item.quantity

            new_order = Order(
                user_id=user_id,
                status=OrderStatus.CREATED,
                promo_code_id=promo_code_obj.id if promo_code_obj else None,
                total_amount=total_amount,
                discount_amount=discount_amount,
            )
            session.add(new_order)
            await session.flush()

            for item in order_create_request.items:
                product = products[item.product_id]
                order_item = OrderItem(
                    order_id=new_order.id,
                    product_id=item.product_id,
                    quantity=item.quantity,
                    price_at_order=product.price
                )
                session.add(order_item)

            if promo_code_obj:
                promo_code_obj.current_uses += 1

            user_op = UserOperation(
                user_id=user_id,
                operation_type=UserOperationType.CREATE_ORDER
            )
            session.add(user_op)

            await session.commit()
            await session.refresh(new_order)

        return new_order.to_pydantic()

    async def orders_id_get(
        self,
        id: StrictInt,
        token_bearer: TokenModel,
    ) -> OrderResponse:
        async with get_session() as session:
            order = await session.get(Order, id)
            if not order:
                error_response = ErrorResponse(
                    error_code='ORDER_NOT_FOUND',
                    message="Продукт не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )
            if order.user_id != token_bearer.user_id and token_bearer.role != UserRole.ADMIN:
                error_response = ErrorResponse(
                    error_code='ORDER_OWNERSHIP_VIOLATION',
                    message='Заказ принадлежит другому пользователю',
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=error_response.model_dump(by_alias=True)
                )
            return order.to_pydantic()

    async def orders_id_put(
        self,
        id: StrictInt,
        order_update_request: OrderUpdateRequest,
        token_bearer: TokenModel,
    ) -> OrderResponse:
        now = datetime.datetime.now()

        async with get_session() as session:
            stmt = (
                select(Order)
                .where(Order.id == id)
                .options(selectinload(Order.items))
                .with_for_update()
            )
            order = await session.scalar(stmt)
            if not order:
                error_response = ErrorResponse(
                    error_code="ORDER_NOT_FOUND",
                    message="Заказ не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )

            if order.user_id != token_bearer.user_id and token_bearer.role != UserRole.ADMIN:
                error_response = ErrorResponse(
                    error_code="ORDER_OWNERSHIP_VIOLATION",
                    message="Заказ принадлежит другому пользователю"
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=error_response.model_dump(by_alias=True)
                )

            if order.status != OrderStatus.CREATED:
                error_response = ErrorResponse(
                    error_code="INVALID_STATE_TRANSITION",
                    message="Обновление разрешено только для заказов в статусе CREATED"
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            last_op_stmt = (
                select(UserOperation)
                .where(
                    UserOperation.user_id == token_bearer.user_id,
                    UserOperation.operation_type == UserOperationType.UPDATE_ORDER
                )
                .order_by(UserOperation.created_at.desc())
                .limit(1)
            )
            last_op = await session.scalar(last_op_stmt)
            if last_op:
                if last_op.created_at > now - ORDER_UPDATE_COOLDOWN:
                    error_response = ErrorResponse(
                        error_code="ORDER_LIMIT_EXCEEDED",
                        message="Слишком частые попытки обновления заказа"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=error_response.model_dump(by_alias=True)
                    )

            old_product_ids = [item.product_id for item in order.items]
            new_product_ids = [item.product_id for item in order_update_request.items]
            all_product_ids = set(old_product_ids + new_product_ids)

            products_stmt = (
                select(Product)
                .where(Product.id.in_(all_product_ids))
                .with_for_update()
            )
            result = await session.execute(products_stmt)
            products = {p.id: p for p in result.scalars().all()}

            promo_code = None
            if order.promo_code_id:
                promo_stmt = (
                    select(PromoCode)
                    .where(PromoCode.id == order.promo_code_id)
                    .with_for_update()
                )
                promo_code = await session.scalar(promo_stmt)

            for old_item in order.items:
                product = products.get(old_item.product_id)
                if product:
                    product.stock += old_item.quantity

            for old_item in order.items:
                await session.delete(old_item)

            insufficient_stock = []
            for item in order_update_request.items:
                product = products.get(item.product_id)
                if not product:
                    # Если продукт не найден (возможно, удалён после блокировки)
                    error_response = ErrorResponse(
                        error_code="PRODUCT_NOT_FOUND",
                        message=f"Товар с ID {item.product_id} не найден"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=error_response.model_dump(by_alias=True)
                    )
                if product.status != ProductStatus.ACTIVE:
                    error_response = ErrorResponse(
                        error_code="PRODUCT_INACTIVE",
                        message=f"Товар {item.product_id} неактивен"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=error_response.model_dump(by_alias=True)
                    )
                if product.stock < item.quantity:
                    insufficient_stock.append(StockIssue(
                        product_id=item.product_id,
                        requested_quantity=item.quantity,
                        available_stock=product.stock
                    ))

            if insufficient_stock:
                error_response = ErrorResponse(
                    error_code="INSUFFICIENT_STOCK",
                    message="Недостаточно товара на складе",
                    details=ErrorResponseDetails(stock_issues=insufficient_stock)
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            for item in order_update_request.items:
                product = products[item.product_id]
                product.stock -= item.quantity

            subtotal = Decimal(0)
            for item in order_update_request.items:
                product = products[item.product_id]
                subtotal += product.price * item.quantity

            discount_amount = Decimal(0)
            total_amount = subtotal
            new_promo_code_id = order.promo_code_id

            if promo_code:
                if not promo_code.active:
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_INACTIVE",
                        message="Промокод неактивен"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=error_response.model_dump(by_alias=True)
                    )
                if not (promo_code.valid_from <= now <= promo_code.valid_until):
                    error_response = ErrorResponse(
                        error_code="PROMO_CODE_EXPIRED",
                        message="Срок действия промокода истёк"
                    )
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=error_response.model_dump(by_alias=True)
                    )

                if subtotal < promo_code.min_order_amount:
                    promo_code.current_uses -= 1
                    new_promo_code_id = None
                else:
                    if promo_code.discount_type == PromoCodeType.PERCENTAGE:
                        discount = subtotal * Decimal(promo_code.discount_value) / Decimal(100)
                        max_discount = subtotal * Decimal("0.7")
                        discount_amount = min(discount, max_discount)
                    else:
                        discount_amount = min(Decimal(promo_code.discount_value), subtotal)
                    total_amount = subtotal - discount_amount

            order.total_amount = total_amount
            order.discount_amount = discount_amount
            order.promo_code_id = new_promo_code_id

            for item in order_update_request.items:
                product = products[item.product_id]
                order_item = OrderItem(
                    order_id=order.id,
                    product_id=item.product_id,
                    quantity=item.quantity,
                    price_at_order=product.price
                )
                session.add(order_item)

            user_op = UserOperation(
                user_id=token_bearer.user_id,
                operation_type=UserOperationType.UPDATE_ORDER
            )
            session.add(user_op)

            await session.commit()
            await session.refresh(order)

        return order.to_pydantic()

    async def orders_id_cancel_post(
            self,
            id: StrictInt,
            token_bearer: TokenModel,
    ) -> OrderResponse:
        async with get_session() as session:
            stmt = (
                select(Order)
                .where(Order.id == id)
                .options(selectinload(Order.items))
                .with_for_update()
            )
            order = await session.scalar(stmt)
            if not order:
                error_response = ErrorResponse(
                    error_code="ORDER_NOT_FOUND",
                    message="Заказ не найден"
                )
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=error_response.model_dump(by_alias=True)
                )

            if order.user_id != token_bearer.user_id and token_bearer.role != UserRole.ADMIN:
                error_response = ErrorResponse(
                    error_code="ORDER_OWNERSHIP_VIOLATION",
                    message="Заказ принадлежит другому пользователю"
                )
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=error_response.model_dump(by_alias=True)
                )

            if order.status not in (OrderStatus.CREATED, OrderStatus.PAYMENT_PENDING):
                error_response = ErrorResponse(
                    error_code="INVALID_STATE_TRANSITION",
                    message="Отмена разрешена только для заказов в статусе CREATED или PAYMENT_PENDING"
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=error_response.model_dump(by_alias=True)
                )

            product_ids = [item.product_id for item in order.items]
            products = {}
            if product_ids:
                products_stmt = (
                    select(Product)
                    .where(Product.id.in_(product_ids))
                    .with_for_update()
                )
                result = await session.execute(products_stmt)
                products = {p.id: p for p in result.scalars().all()}

            promo_code = None
            if order.promo_code_id:
                promo_stmt = (
                    select(PromoCode)
                    .where(PromoCode.id == order.promo_code_id)
                    .with_for_update()
                )
                promo_code = await session.scalar(promo_stmt)

            for item in order.items:
                product = products.get(item.product_id)
                if product:
                    product.stock += item.quantity

            if promo_code:
                promo_code.current_uses -= 1

            order.status = OrderStatus.CANCELED

            await session.commit()
            await session.refresh(order)

        return order.to_pydantic()
