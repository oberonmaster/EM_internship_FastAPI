import json
from decimal import Decimal
from datetime import date, datetime, time, timedelta
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from redis import Redis
from sqlalchemy import select

from DB_interface import SpimexTradingResult, async_session, get_dynamics, get_last_trading_date, get_trading_results

app = FastAPI(title="Spimex trading API",
              description="API для выдачи данных из таблицы spimex_trading_results. Кэш сохраняется до 14:11, " 
                          "после этого происходит инвалидация кэша.",
              version="1.0")


# ---------- Redis init ----------
try:
    redis = Redis(host='localhost', port=6379, db=0, decode_responses=False)
    redis.ping()
except Exception as e:
    print(f"Redis connection error: {e}")
    redis = None


def seconds_until_next_invalidation() -> int:
    now = datetime.now()
    today_target = now.replace(hour=14,
                               minute=11,
                               second=0,
                               microsecond=0)
    if now < today_target:
        delta = today_target - now
    else:
        tomorrow_target = today_target + timedelta(days=1)
        delta = tomorrow_target - now
    return int(delta.total_seconds())


def invalidate_cache_if_needed():
    """Сбросит Redis один раз после пересечения порога 14:11 (сохраняет метку даты)."""
    if not redis:
        return
    try:
        now = datetime.now()
        today_str = now.date().isoformat()
        target_time = now.replace(hour=14,
                                  minute=11,
                                  second=0,
                                  microsecond=0)
        last = redis.get("last_invalidation_date")
        last = last.decode() if last else None

        # Если сейчас уже после целевого времени и сброс ещё не делался сегодня -> flush
        if now >= target_time and last != today_str:
            # flush only cache DB (be careful on prod; лучше ключи с префиксом). Здесь мы предполагаем отдельную БД Redis.
            try:
                redis.flushdb()
                redis.set("last_invalidation_date", today_str)
                print("Redis cache flushed due to daily invalidation.")
            except Exception as e:
                print("Failed to flush redis:", e)
    except Exception as e:
        print("invalidate_cache_if_needed error:", e)


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
        orm_mode = True


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_cache(key: str) -> Optional[Any]:
    if not redis:
        return None
    try:
        raw = redis.get(key)
        if not raw:
            return None
        # redis stored json bytes
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)
    except Exception as e:
        print(f"Cache get error: {e}")
        return None


def set_cache(key: str, value: Any) -> None:
    """Сохраняем в Redis с TTL до следующей инвалидации (до next 14:11)."""
    if not redis:
        return
    try:
        ttl = seconds_until_next_invalidation()
        json_value = json.dumps(value, cls=CustomJSONEncoder)
        redis.setex(key, ttl, json_value)
    except Exception as e:
        print(f"Cache set error: {e}")


def model_to_serializable(m) -> Dict[str, Any]:
    """Преобразуем объект SQLAlchemy в serializable dict (Decimal->float, date->iso)."""
    out = {}
    for col in m.__table__.columns:
        val = getattr(m, col.name)
        if isinstance(val, Decimal):
            out[col.name] = float(val)
        elif isinstance(val, (date, datetime)):
            out[col.name] = val.isoformat()
        else:
            out[col.name] = val
    return out


def cache_invalidation_dep():
    invalidate_cache_if_needed()

# ---------- Endpoints ----------
@app.get("/last_dates", response_model=List[date], summary="Последние даты торгов")
async def get_last_trading_dates(limit: int = Query(10, ge=1, le=365, description="Количество последних дат")):
    """
    Возвращает список уникальных последних дат торгов (по убыванию).
    Параметр:
      - limit (опционально, default=10) — сколько последних дат вернуть.
    """
    cache_key = f"last_dates:{limit}"
    # проверяем кэш (и запускаем инвалидацию)
    cache_invalidation_dep()
    cached = get_cache(cache_key)
    if cached is not None:
        return [date.fromisoformat(d) for d in cached]

    try:
        async with async_session() as session:
            result = await session.execute(
                select(SpimexTradingResult.date).distinct().order_by(SpimexTradingResult.date.desc()).limit(limit)
            )
            dates = [row[0] for row in result.all()]
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

    set_cache(cache_key, [d.isoformat() for d in dates])
    return dates

@app.get("/results", response_model=List[TradingResult], summary="Результаты торгов (фильтрация)")
async def api_get_trading_results(
        oil_id: Optional[str] = Query(None, description="4-значный код товара, например 'URAL'"),
        delivery_type_id: Optional[str] = Query(None, description="тип поставки (последний символ exchange_product_id)"),
        delivery_basis_id: Optional[str] = Query(None, description="код базы доставки (3 символа, позиции 5-7)"),
        date_value: Optional[date] = Query(None, description="Фильтр по точной дате (YYYY-MM-DD)"),
        limit: int = Query(100, ge=1, le=1000, description="Лимит записей для выдачи"),
        _ = Depends(cache_invalidation_dep)
):
    """
    Получение результатов торгов с гибкой фильтрацией.
    Параметры:
      - oil_id: опционально
      - delivery_type_id: опционально
      - delivery_basis_id: опционально
      - date_value: опционально
      - limit: опционально
    """
    cache_key = f"results:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{date_value}:{limit}"
    cached = get_cache(cache_key)
    if cached is not None:
        return cached

    try:
        results = await get_trading_results(limit=limit, oil_id=oil_id,
                                            delivery_type_id=delivery_type_id,
                                            delivery_basis_id=delivery_basis_id,
                                            date_value=date_value)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

    results_serialized = [model_to_serializable(r) for r in results]
    set_cache(cache_key, results_serialized)
    return results_serialized


@app.get("/dynamics", response_model=List[TradingResult], summary="Динамика торгов за период")
async def get_dynamics_api(
        start_date: date = Query(..., description="Дата начала периода (YYYY-MM-DD) — обязательна"),
        end_date: date = Query(..., description="Дата конца периода (YYYY-MM-DD) — обязательна"),
        oil_id: Optional[str] = Query(None, description="Фильтр по oil_id (опционально)"),
        delivery_type_id: Optional[str] = Query(None, description="Фильтр по delivery_type_id (опционально)"),
        delivery_basis_id: Optional[str] = Query(None, description="Фильтр по delivery_basis_id (опционально)"),
        limit: Optional[int] = Query(None, ge=1, le=10000, description="При желании ограничить количество результатов"),
        _ = Depends(cache_invalidation_dep)
):
    """
    Возвращает список сделок за период [start_date, end_date].
    start_date и end_date — обязательны
    Остальные параметры — опциональны и уточняют выборку.
    """
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    cache_key = f"dynamics:{start_date}:{end_date}:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{limit}"
    cached = get_cache(cache_key)
    if cached is not None:
        return cached

    try:
        results = await get_dynamics(start_date=start_date,
                                     end_date=end_date,
                                     oil_id=oil_id,
                                     delivery_type_id=delivery_type_id,
                                     delivery_basis_id=delivery_basis_id,
                                     limit=limit)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500,
                            detail=str(e))

    results_serialized = [model_to_serializable(r) for r in results]
    set_cache(cache_key, results_serialized)
    return results_serialized


@app.get("/last_results", response_model=List[TradingResult], summary="Результаты за последнюю дату торгов")
async def get_last_trading_results(_ = Depends(cache_invalidation_dep)):
    """
    Возвращает все записи за последнюю дату торгов (определяется автоматически).
    """
    cache_key = "last_results"
    cached = get_cache(cache_key)
    if cached is not None:
        return cached

    last_date = await get_last_trading_date()
    if not last_date:
        raise HTTPException(status_code=404, detail="No trading data found")

    try:
        results = await get_trading_results(limit=10000, date_value=last_date)
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

    results_serialized = [model_to_serializable(r) for r in results]
    set_cache(cache_key, results_serialized)
    return results_serialized


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)