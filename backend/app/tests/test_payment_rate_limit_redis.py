"""
LAB 05: Rate limiting endpoint оплаты через Redis.
"""
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

import httpx
from app.main import app

@pytest.mark.asyncio
async def test_payment_endpoint_rate_limit():
    """
    TODO: Реализовать тест.

    Рекомендуемая проверка:
    1) Сделать N запросов оплаты в пределах одного окна.
    2) Проверить, что первые <= limit проходят.
    3) Следующие запросы получают 429 Too Many Requests.
    4) Проверить заголовки X-RateLimit-Limit / X-RateLimit-Remaining.
    """
    #raise NotImplementedError("TODO: implement redis rate limiting test")
    call_cnt = 0

    async def mock_incr(key):
        nonlocal call_cnt
        call_cnt += 1
        return call_cnt

    async def mock_expire(key, ttl):
        pass 

    mock_redis = AsyncMock()
    mock_redis.incr.side_effect = mock_incr
    mock_redis.expire.side_effect = mock_expire

    with patch("app.middleware.rate_limit_middleware.get_redis", return_value=mock_redis):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            responses = []
            for i in range(6):
                r = await client.post(
                    "/api/payments/retry-demo",
                    json={"order_id": "00000000-0000-0000-0000-000000000001", "mode": "unsafe"},
                    headers={"X-User-Id": "test-user-123"},
                )

                responses.append(r)

    for i, r in enumerate(responses[:5]):
        assert r.status_code != 429, f"Запрос {i+1} неожиданно получил 429"
        assert "X-RateLimit-Limit" in r.headers
        assert "X-RateLimit-Remaining" in r.headers

    assert responses[5].status_code == 429, "Ожидали 429 на 6-м запросе!"
    assert responses[5].json()["detail"] == "Rate limit exceeded. Try again later."

    print("\n[RATE LIMIT DEMO]")
    for i, r in enumerate(responses):
        status = r.status_code
        remaining = r.headers.get("X-RateLimit-Remaining", "N/A")
        print(f"Запрос {i+1}: HTTP {status}, X-RateLimit-Remaining={remaining}")
