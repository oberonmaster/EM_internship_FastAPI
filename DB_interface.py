import os
from sqlalchemy import select

import pandas as pd
import asyncio

from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from datetime import datetime

load_dotenv()
DB_NAME=os.getenv("DB_NAME")
DB_HOST=os.getenv("DB_HOST")
DB_PORT=os.getenv("DB_PORT")
DB_USER=os.getenv("DB_USER")
DB_PASS=os.getenv("DB_PASS")

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

        # Находим строку с метрическими тоннами
        metric_ton_row = None
        for i in range(len(df)):
            if isinstance(df.iloc[i, 1], str) and 'Единица измерения: Метрическая тонна' in df.iloc[i, 1]:
                metric_ton_row = i
                break

        if metric_ton_row is None:
            print(f"Не найдена строка с метрическими тоннами в файле {filename}")
            return

        # Определяем индексы колонок
        col_indices = {
            'code': 1, 'name': 2, 'basis': 3,
            'volume': 4, 'total': 5, 'count': 14
        }

        # Собираем данные для сохранения
        data_to_save = []
        for i in range(metric_ton_row + 3, len(df)):
            row = df.iloc[i]

            # Пропускаем строки с "Итого"
            if isinstance(row[col_indices['code']], str) and ('Итого:' in row[col_indices['code']] or
                                                              'Итого по секции:' in row[col_indices['code']]):
                continue

            # Пропускаем пустые строки
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

        # Сохраняем данные в БД
        if data_to_save:
            async with async_session() as session:
                # Проверяем существование записей
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