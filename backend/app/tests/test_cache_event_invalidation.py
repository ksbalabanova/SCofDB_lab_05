"""
LAB 05: Проверка починки через событийную инвалидацию.
"""
import pytest
import json
from unittest.mock import AsyncMock, call, MagicMock, patch

from app.application.cache_service import CacheService
from app.application.cache_events import CacheInvalidationEventBus, OrderUpdatedEvent
from app.infrastructure.cache_keys import order_card_key, catalog_key


@pytest.mark.asyncio
async def test_order_card_is_fresh_after_event_invalidation():
    """
    TODO: Реализовать сценарий:
    1) Прогреть кэш карточки заказа.
    2) Изменить заказ через mutate-with-event-invalidation.
    3) Убедиться, что ключ карточки инвалидирован.
    4) Повторный GET возвращает свежие данные из БД, а не stale cache.
    """
    #raise NotImplementedError("TODO: implement event invalidation freshness test")
    order_id = "00000000-0000-0000-0000-000000000001"

    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    mock_order_row = MagicMock()
    mock_order_row.fetchone.return_value = (
        order_id,
        "00000000-0000-0000-0000-000000000002",
        "created",
        1.0,
        "2020-10-10 00:00:00",
    )
    mock_items_row = MagicMock()
    mock_items_row.fetchall.return_value = []

    mock_db = AsyncMock()
    mock_db.execute.side_effect = [mock_order_row, mock_items_row]

    cache_svc = CacheService(redis_client=mock_redis, db_session=mock_db)
    result1 = await cache_svc.get_order_card(order_id, use_cache=True)

    assert result1["_source"] == "db"
    assert result1["total_amount"] == 1.0

    mock_redis.setex.assert_called_once()

    event_bus = CacheInvalidationEventBus(cache_service=cache_svc)
    event = OrderUpdatedEvent(order_id=order_id)
    await event_bus.publish_order_updated(event)

    deleted_keys = [c.args[0] for c in mock_redis.delete.call_args_list]
    assert order_card_key(order_id) in deleted_keys, "Ключ карточки не инвалидирован"
    assert catalog_key() in deleted_keys, "Ключ каталога не инвалидирован"

    print("\nTest 2")
    print(f"Ключи удалены из Redis: {deleted_keys}")
    mock_redis.get.return_value = None

    fresh_order_row = MagicMock()
    fresh_order_row.fetchone.return_value = (
        order_id,
        "00000000-0000-0000-0000-000000000002",
        "created",
        777.0,
        "2020-11-11 00:00:00",
    )
    fresh_items_row = MagicMock()
    fresh_items_row.fetchall.return_value = []
    mock_db.execute.side_effect = [fresh_order_row, fresh_items_row]

    result2 = await cache_svc.get_order_card(order_id, use_cache=True)

    assert result2["_source"] == "db"
    assert result2["total_amount"] == 777.0, "Ожидали свежие данные из БД (777.0)"

    print(f"После инвалидации total_amount = {result2['total_amount']}")
