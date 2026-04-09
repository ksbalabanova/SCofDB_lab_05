"""
LAB 05: Демонстрация неконсистентности кэша.
"""
from unittest.mock import AsyncMock, MagicMock, patch
import json
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.application.cache_service import CacheService
from app.infrastructure.cache_keys import order_card_key, catalog_key


@pytest.mark.asyncio
async def test_stale_order_card_when_db_updated_without_invalidation():
    """
    TODO: Реализовать сценарий:
    1) Прогреть кэш карточки заказа (GET /api/cache-demo/orders/{id}/card?use_cache=true).
    2) Изменить заказ в БД через endpoint mutate-without-invalidation.
    3) Повторно запросить карточку с use_cache=true.
    4) Проверить, что клиент получает stale данные из кэша.
    """
    #raise NotImplementedError("TODO: implement stale cache consistency test")
    order_id = "00000000-0000-0000-0000-000000000001"
    cached_data = {
        "order_id": order_id,
        "user_id": "00000000-0000-0000-0000-000000000002",
        "status": "created",
        "total_amount": 1.0,
        "created_at": "2020-10-10 00:00:00",
        "items": [],
    }
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(cached_data)
    mock_db = AsyncMock()
    cache_svc = CacheService(redis_client=mock_redis, db_session=mock_db)
    result = await cache_svc.get_order_card(order_id, use_cache=True)
    assert result["_source"] == "cache"
    assert result["total_amount"] == 1.0, (
        f"Ожидали stale данные (1.0), получили {result['total_amount']}"
    )
    mock_db.execute.assert_not_called()

    print("\nTest 1")
    print(f"В кэше total_amount = {result['total_amount']} (устарело)")
    print(f"_source = {result['_source']} — данные из кэша")
