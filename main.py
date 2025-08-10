import asyncio
import datetime
import os
import aiohttp

from DB_interface import create_tables, parse_to_db

# https://spimex.com/upload/reports/oil_xls/oil_xls_20250722162000.xls
# Дата торгов: 22.07.2025
# url generation
# reporting_date = datetime.datetime.now().strftime("%Y%m%d")  # 20250722
# url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{reporting_date}162000.xls"


filenames = []
async def download_files(session, url):
    semaphore = asyncio.Semaphore(5)
    async with semaphore:
        filename = os.path.join(url.split("/")[-1])
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    filenames.append(filename)

                    with open(filename, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)
                    print(f"Успешно: {filename}")
                else:
                    print(f"Ошибка {response.status}: {url}")
        except Exception as e:
            print(f"Ошибка при загрузке {url}: {str(e)}")


async def main():
    # создание БД
    await create_tables()

    # загрузка файлов
    start_date = datetime.datetime.strptime("20250701", "%Y%m%d")
    end_date = datetime.datetime.now()
    curent_date = start_date
    urls = []
    while curent_date <= end_date:
        reporting_date = curent_date.strftime("%Y%m%d")
        url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{reporting_date}162000.xls"
        urls.append(url)
        curent_date += datetime.timedelta(days=1)

    async with aiohttp.ClientSession() as session:
        tasks = [download_files(session, url) for url in urls]
        await asyncio.gather(*tasks)


    # заполнение бд
    for filename in filenames:
        await parse_to_db(filename)


if __name__ == "__main__":
    asyncio.run(main())
    # print(filenames)
