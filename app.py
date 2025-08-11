from decimal import Decimal
import json
from datetime import date, datetime, time
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from redis import Redis
from DB_interface import SpimexTradingResult, async_session, get_dynamics, get_last_trading_date

app = FastAPI()

# Инициализация Redis с обработкой ошибок
try:
    redis = Redis(host='localhost', port=6379, db=0)
    redis.ping()  # Проверка подключения
except Exception as e:
    print(f"Redis connection error: {e}")
    redis = None


def is_after_invalidation_time():
    """Проверяем, прошло ли время инвалидации кэша (14:11)"""
    return datetime.now().time() > time(14, 11)


class TradingResult(BaseModel):
    id: int
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: float
    total: float
    count: int
    date: date

    class Config:
        from_attributes = True


class CustomJSONEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки Decimal и date"""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_cache(key: str) -> Optional[str]:
    """Безопасное получение данных из кэша"""
    if not redis:
        return None
    try:
        return redis.get(key)
    except Exception as e:
        print(f"Cache get error: {e}")
        return None


def set_cache(key: str, value: Any, ttl: int = 86400) -> None:
    """Безопасное сохранение в кэш с обработкой специальных типов"""
    if not redis:
        return
    try:
        redis.setex(key, ttl, json.dumps(value, cls=CustomJSONEncoder))
    except Exception as e:
        print(f"Cache set error: {e}")


def model_to_dict(model_instance) -> Dict[str, Any]:
    """Конвертируем SQLAlchemy модель в словарь"""
    result = {}
    for column in model_instance.__table__.columns:
        value = getattr(model_instance, column.name)
        result[column.name] = value
    return result


@app.get("/last_dates", response_model=List[date])
async def get_last_trading_dates(limit: int = 10):
    """Получение списка последних торговых дат"""
    cache_key = f"last_dates:{limit}"

    # Проверка кэша
    if not is_after_invalidation_time():
        cached = get_cache(cache_key)
        if cached:
            return [date.fromisoformat(d) for d in json.loads(cached)]

    # Запрос к БД
    async with async_session() as session:
        result = await session.execute(
            select(SpimexTradingResult.date)
            .distinct()
            .order_by(SpimexTradingResult.date.desc())
            .limit(limit)
        )
        dates = [row[0] for row in result.all()]

    # Сохраняем в кэш
    set_cache(cache_key, dates)
    return dates


@app.get("/results", response_model=List[TradingResult])
async def get_trading_results(
        oil_id: Optional[str] = None,
        date: Optional[date] = None,
        limit: int = 100
):
    """Получение результатов торгов с фильтрацией"""
    cache_key = f"results:{oil_id}:{date}:{limit}"

    # Проверка кэша
    if not is_after_invalidation_time():
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached)

    # Запрос к БД
    async with async_session() as session:
        query = select(SpimexTradingResult).limit(limit)
        if oil_id:
            query = query.where(SpimexTradingResult.oil_id == oil_id)
        if date:
            query = query.where(SpimexTradingResult.date == date)

        result = await session.execute(query)
        results = result.scalars().all()
        results_dict = [model_to_dict(r) for r in results]

    # Сохраняем в кэш
    set_cache(cache_key, results_dict)
    return results


@app.get("/dynamics", response_model=List[TradingResult])
async def get_dynamics_api(
        start_date: date,
        end_date: date,
        oil_id: Optional[str] = None
):
    """Получение динамики торгов за период"""
    cache_key = f"dynamics:{start_date}:{end_date}:{oil_id}"

    # Проверка кэша
    if not is_after_invalidation_time():
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached)

    # Запрос к БД
    results = await get_dynamics(start_date, end_date, oil_id)
    results_dict = [model_to_dict(r) for r in results]

    # Сохраняем в кэш
    set_cache(cache_key, results_dict)
    return results


@app.get("/last_results", response_model=List[TradingResult])
async def get_last_trading_results():
    """Получение результатов последних торгов"""
    cache_key = "last_results"

    # Проверка кэша
    if not is_after_invalidation_time():
        cached = get_cache(cache_key)
        if cached:
            return json.loads(cached)

    # Получаем последнюю дату
    last_date = await get_last_trading_date()
    if not last_date:
        raise HTTPException(status_code=404, detail="No trading data found")

    # Запрашиваем данные за последнюю дату
    async with async_session() as session:
        result = await session.execute(
            select(SpimexTradingResult)
            .where(SpimexTradingResult.date == last_date)
        )
        results = result.scalars().all()
        results_dict = [model_to_dict(r) for r in results]

    # Сохраняем в кэш
    set_cache(cache_key, results_dict)
    return results


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)