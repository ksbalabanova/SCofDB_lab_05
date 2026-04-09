"""Cache consistency demo endpoints for LAB 05."""

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.infrastructure.db import get_db
from app.infrastructure.redis_client import get_redis
from app.application.cache_service import CacheService
from app.application.cache_events import CacheInvalidationEventBus, OrderUpdatedEvent


router = APIRouter(prefix="/api/cache-demo", tags=["cache-demo"])


class UpdateOrderRequest(BaseModel):
    """Payload для изменения заказа в demo-сценариях."""

    new_total_amount: float


@router.get("/catalog")
async def get_catalog(use_cache: bool = True, db: AsyncSession = Depends(get_db)) -> Any:
    """
    TODO: Кэш каталога товаров в Redis.

    Требования:
    1) При use_cache=true читать/писать Redis.
    2) При cache miss грузить из БД и класть в кэш.
    3) Добавить TTL.

    Примечание:
    В текущей схеме можно строить \"каталог\" как агрегат по order_items.product_name.
    """
    #raise HTTPException(status_code=501, detail="TODO: implement catalog cache")
    result = await cache_svc.get_catalog(use_cache=use_cache)
    result["use_cache"] = use_cache
    return result


@router.get("/orders/{order_id}/card")
async def get_order_card(
    order_id: uuid.UUID,
    use_cache: bool = True,
    db: AsyncSession = Depends(get_db),
) -> Any:
    """
    TODO: Кэш карточки заказа в Redis.

    Требования:
    1) Ключ вида order_card:v1:{order_id}.
    2) При use_cache=true возвращать данные из кэша.
    3) При miss грузить из БД и сохранять в кэш.
    """
    #raise HTTPException(status_code=501, detail="TODO: implement order card cache")
    data = await cache_svc.get_order_card(str(order_id), use_cache=use_cache)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return data


@router.post("/orders/{order_id}/mutate-without-invalidation")
async def mutate_without_invalidation(
    order_id: uuid.UUID,
    payload: UpdateOrderRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    TODO: Намеренно сломанный сценарий консистентности.

    Нужно:
    1) Изменить заказ в БД.
    2) НЕ инвалидировать кэш.
    3) Показать, что последующий GET /orders/{id}/card может вернуть stale data.
    """
    #raise HTTPException(status_code=501, detail="TODO: implement stale cache demo")
    db = cache_svc.db  
    result = await db.execute(
        text("""
            UPDATE orders
            SET total_amount = :amount
            WHERE id = :id
            RETURNING id, total_amount
        """),
        {"amount": payload.new_total_amount, "id": str(order_id)},
    )
    row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    return {
        "order_id": str(order_id),
        "new_total_amount": float(row[1]),
        "cache_invalidated": False,
        "warning": "Cache not invalidated",
    }


@router.post("/orders/{order_id}/mutate-with-event-invalidation")
async def mutate_with_event_invalidation(
    order_id: uuid.UUID,
    payload: UpdateOrderRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    TODO: Починка через событийную инвалидацию.

    Нужно:
    1) Изменить заказ в БД.
    2) Сгенерировать событие OrderUpdated.
    3) Обработчик события должен инвалидировать связанные cache keys:
       - order_card:v1:{order_id}
       - catalog:v1 (если изменение влияет на каталог/агрегаты)
    """
    #raise HTTPException(status_code=501, detail="TODO: implement event invalidation")
    db = cache_svc.db
    result = await db.execute(
        text("""
            UPDATE orders
            SET total_amount = :amount
            WHERE id = :id
            RETURNING id, total_amount
        """),
        {"amount": payload.new_total_amount, "id": str(order_id)},
    )
    row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    event = OrderUpdatedEvent(order_id=str(order_id))
    event_bus = CacheInvalidationEventBus(cache_service=cache_svc)
    await event_bus.publish_order_updated(event)

    return {
        "order_id": str(order_id),
        "new_total_amount": float(row[1]),
        "cache_invalidated": True,
        "message": "Cache invalidated",
    }
