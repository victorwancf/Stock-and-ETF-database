#IEX Cloud console: https://iexcloud.io/console
#IEX Cloud API: https://iexcloud.io/docs/api/

import config
import json
import requests
import time
from datetime import datetime
import aiohttp, asyncpg, asyncio

async def get_price(pool, stock_id, url):
    async with pool.acquire() as connection:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url) as response:
                resp = await response.read()
                response = json.loads(resp)

                params = [(stock_id, datetime.strptime(bar['date'], '%Y-%m-%d'), round(bar['open'], 2),
                            round(bar['high'], 2), round(bar['low'], 2), round(bar['close'], 2), round(bar['volume'],0)) for bar in
                            response]
                await connection.copy_records_to_table('stock_price', records=params)
                # INSERT INTO stock_price (stock_id, dt, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7)
                # """, params)

    except Exception as e:
        print("Unable to get url {} due to {}.".format(url, e.__class__))


async def get_prices(pool, symbol_urls):
    try:
        # schedule aiohttp requests to run concurrently for all symbols
        ret = await asyncio.gather(*[get_price(pool, stock_id, symbol_urls[stock_id]) for stock_id in symbol_urls])
        print("Finalized all. Returned  list of {} outputs.".format(len(ret)))
    except Exception as e:
        print(e)


async def get_stocks():
    # create database connection pool, to make sure many connections can be happened in the same time
    pool = await asyncpg.create_pool(user=config.DB_USER, password=config.DB_PASS, database=config.DB_NAME,
                                     host=config.DB_HOST, command_timeout=60)

    # get a connection
    async with pool.acquire() as connection:
        stocks = await connection.fetch("SELECT * FROM stock WHERE id IN (SELECT holding_id FROM etf_holding)")

        symbol_urls = {}
        for stock in stocks:
            symbol_urls[stock['id']] = f"https://cloud.iexapis.com/v1/stock/{stock['symbol']}/chart/1m/?token={config.API_KEY}" #Can we get 5 min data? or need to apply limit?

    await get_prices(pool, symbol_urls)

start = time.time()

asyncio.run(get_stocks())

end = time.time()

print("Took {} seconds.".format(end - start))