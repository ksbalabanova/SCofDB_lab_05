"""Cache service template for LAB 05."""
import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis

from app.infrastructure.cache_keys import catalog_key, order_card_key

CATALOG_TTL = 60
ORDER_CARD_TTL = 60


class CacheService:
    """
    Сервис кэширования каталога и карточки заказа.

    TODO:
    - реализовать методы через Redis client + БД;
    - добавить TTL и версионирование ключей.
    """

    def __init__(self, redis_client: Redis, db_session: AsyncSession):
        self.redis = redis_client
        self.db = db_session


    async def get_catalog(self, *, use_cache: bool = True) -> list[dict[str, Any]]:
        """
        TODO:
        1) Попытаться вернуть catalog из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        #raise NotImplementedError("TODO: implement get_catalog")
        key = catalog_key()
        if use_cache:
            cached = await self.redis.get(key)
            if cached is not None:
                data = json.loads(cached)
                data_src = {"items": data, "_source": "cache"}
                return data_src

        res = await self.db.execute(text("""
            SELECT product_name, 
                COUNT(*) AS order_count,
                AVG(price) AS avg_price,
                SUM(quantity) AS total_qty
            FROM order_items
            GROUP BY product_name
            ORDER BY product_name
        """))
        rows = res.fetchall()
        catalog = [
            {
                "product_name": row[0],
                "order_count":  int(row[1]),
                "avg_price":    float(row[2]),
                "total_qty":    int(row[3]),
            }
            for row in rows
        ]
        if use_cache:
            await self.redis.setex(key, CATALOG_TTL, json.dumps(catalog))

        return {"items": catalog, "_source": "db"}


    async def get_order_card(self, order_id: str, *, use_cache: bool = True) -> dict[str, Any]:
        """
        TODO:
        1) Попытаться вернуть карточку заказа из Redis.
        2) При miss загрузить из БД.
        3) Положить в Redis с TTL.
        """
        #raise NotImplementedError("TODO: implement get_order_card")
        key = order_card_key(order_id)
        if use_cache:
            cached = await self.redis.get(key)
            if cached is not None:
                data = json.loads(cached)
                data["_source"] = "cache"
                return data
        
        order_row = await self.db.execute(
            text("""
                SELECT id, user_id, status, total_amount, created_at
                FROM orders
                WHERE id = :id
            """),
            {"id": order_id},
        )
        order = order_row.fetchone()
        if order is None:
            return None 

        items_result = await self.db.execute(
            text("""
                SELECT id, product_name, price, quantity, subtotal
                FROM order_items
                WHERE order_id = :order_id
            """),
            {"order_id": order_id},
        )
        item_rows = items_result.fetchall()

        data = {
            "order_id": str(order[0]),
            "user_id": str(order[1]),
            "status": order[2],
            "total_amount": float(order[3]),
            "created_at": str(order[4]),
            "items": [
                {
                    "id": str(ir[0]),
                    "product_name": ir[1],
                    "price": float(ir[2]),
                    "quantity": ir[3],
                    "subtotal": float(ir[4]),
                }
                for ir in item_rows
            ],
        }
        if use_cache:
            await self.redis.setex(key, ORDER_CARD_TTL, json.dumps(data))

        data["_source"] = "db"
        return data

    async def invalidate_order_card(self, order_id: str) -> None:
        """TODO: Удалить ключ карточки заказа из Redis."""
        await self.redis.delete(order_card_key(order_id))
        #raise NotImplementedError("TODO: implement invalidate_order_card")

    async def invalidate_catalog(self) -> None:
        """TODO: Удалить ключ каталога из Redis."""
        await self.redis.delete(catalog_key())
        #raise NotImplementedError("TODO: implement invalidate_catalog")
