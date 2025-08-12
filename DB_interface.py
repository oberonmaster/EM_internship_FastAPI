import os
from sqlalchemy import select, func, and_
import pandas as pd
import asyncio
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime, date

load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

db_url = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()

class SpimexTradingResult(Base):
    __tablename__ = 'spimex_trading_results'
    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Numeric)
    total = Column(Numeric)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime, default=datetime.utcnow)
    updated_on = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


engine = create_async_engine(db_url, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def parse_to_db(filename):
    try:
        # Извлекаем дату из имени файла
        date_str = filename.split('_')[-1][:8]
        trade_date = datetime.strptime(date_str, '%Y%m%d').date()

        # Читаем Excel файл через pandas (в отдельном потоке)
        df = await asyncio.to_thread(pd.read_excel, filename, sheet_name='TRADE_SUMMARY', header=None)

        # Поиск стартовой строки с метрическими тоннами
        metric_ton_row = None
        for i in range(len(df)):
            if isinstance(df.iloc[i, 1], str) and 'Единица измерения: Метрическая тонна' in df.iloc[i, 1]:
                metric_ton_row = i
                break

        if metric_ton_row is None:
            print(f"Не найдена строка с метрическими тоннами в файле {filename}")
            return

        # Определяем индексы колонок (возможно нужно править под структуру)
        col_indices = {
            'code': 1, 'name': 2, 'basis': 3,
            'volume': 4, 'total': 5, 'count': 14
        }

        data_to_save = []
        for i in range(metric_ton_row + 3, len(df)):
            row = df.iloc[i]

            # Пропускаем суммарные строки
            if isinstance(row[col_indices['code']], str) and ('Итого:' in row[col_indices['code']] or
                                                              'Итого по секции:' in row[col_indices['code']]):
                continue

            if (pd.isna(row[col_indices['code']]) or
                    (isinstance(row[col_indices['code']], str) and row[col_indices['code']].strip() == '-') or
                    pd.isna(row[col_indices['count']]) or
                    (isinstance(row[col_indices['count']], str) and row[col_indices['count']].strip() == '-')):
                continue

            try:
                count = int(row[col_indices['count']]) if not pd.isna(row[col_indices['count']]) else 0
                if count <= 0:
                    continue

                exchange_product_id = str(row[col_indices['code']]).strip()
                exchange_product_name = str(row[col_indices['name']]).strip()
                delivery_basis_name = str(row[col_indices['basis']]).strip()

                volume = float(str(row[col_indices['volume']]).replace(' ', '')) if not pd.isna(
                    row[col_indices['volume']]) else 0
                total = float(str(row[col_indices['total']]).replace(' ', '')) if not pd.isna(
                    row[col_indices['total']]) else 0

                data_to_save.append({
                    'exchange_product_id': exchange_product_id,
                    'exchange_product_name': exchange_product_name,
                    'oil_id': exchange_product_id[:4],
                    'delivery_basis_id': exchange_product_id[4:7],
                    'delivery_basis_name': delivery_basis_name,
                    'delivery_type_id': exchange_product_id[-1],
                    'volume': volume,
                    'total': total,
                    'count': count,
                    'date': trade_date
                })
            except Exception as e:
                print(f"Ошибка при обработке строки {i + 1}: {e}")
                continue

        # Сохраняем данные в БД, проверяя дубликаты
        if data_to_save:
            async with async_session() as session:
                for item in data_to_save:
                    result = await session.execute(
                        select(SpimexTradingResult).filter_by(
                            exchange_product_id=item['exchange_product_id'],
                            date=item['date']
                        )
                    )
                    if not result.scalars().first():
                        session.add(SpimexTradingResult(**item))
                await session.commit()
                print(f"Файл {filename} обработан, добавлено {len(data_to_save)} записей")

    except Exception as e:
        print(f"Ошибка при обработке файла {filename}: {e}")


async def get_last_trading_date():
    """Получаем последнюю дату торгов"""
    async with async_session() as session:
        result = await session.execute(select(func.max(SpimexTradingResult.date)))
        return result.scalar()


async def get_dynamics(start_date: date, end_date: date, oil_id: str = None,
                       delivery_type_id: str = None, delivery_basis_id: str = None, limit: int = None):
    """
    Получаем динамику за период с возможностью фильтрации по oil_id, delivery_type_id, delivery_basis_id.
    start_date и end_date обязателны — это основной смысл метода 'dynamics'.
    """
    async with async_session() as session:
        conditions = [SpimexTradingResult.date.between(start_date, end_date)]
        if oil_id:
            conditions.append(SpimexTradingResult.oil_id == oil_id)
        if delivery_type_id:
            conditions.append(SpimexTradingResult.delivery_type_id == delivery_type_id)
        if delivery_basis_id:
            conditions.append(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

        query = select(SpimexTradingResult).where(and_(*conditions)).order_by(SpimexTradingResult.date.asc())
        if limit:
            query = query.limit(limit)

        result = await session.execute(query)
        return result.scalars().all()


async def get_trading_results(limit: int = 100, oil_id: str = None,
                              delivery_type_id: str = None, delivery_basis_id: str = None, date_value: date = None):
    """
    Последние торговые результаты. Параметры фильтрации опциональны:
    - date_value — если указан, вернёт записи только за дату
    - иначе вернёт последние по дате записи (внутри limit)
    """
    async with async_session() as session:
        query = select(SpimexTradingResult)
        if date_value:
            query = query.where(SpimexTradingResult.date == date_value)
        if oil_id:
            query = query.where(SpimexTradingResult.oil_id == oil_id)
        if delivery_type_id:
            query = query.where(SpimexTradingResult.delivery_type_id == delivery_type_id)
        if delivery_basis_id:
            query = query.where(SpimexTradingResult.delivery_basis_id == delivery_basis_id)

        query = query.order_by(SpimexTradingResult.date.desc()).limit(limit)
        result = await session.execute(query)
        return result.scalars().all()
