import datetime as dt
import asyncio

import asyncpg

from data_source import load_thetadata, load_yf
from db_main import get_all_main
from db_constants import fixings_table_name, fixings_database
from db_stuff import connect_to_database, insert_entry, get_ticker


async def update(ticker, date_from, date_to):
    pool = await connect_to_database(database=fixings_database)
    try:
        async with pool.acquire() as conn:
            out_pre = await get_ticker(conn=conn, table_name=fixings_table_name, ticker=ticker)
            print(out_pre)

            # out_data = load_yf(ticker=ticker, date_from=date_from, date_to=date_to)
            out_data = load_thetadata(ticker=ticker, date_from=date_from, date_to=date_to)
            for line in out_data:
                print(line)
                try:
                    entry_out = await insert_entry(conn=conn, table_name=fixings_table_name, ticker=ticker, **line)
                    print(entry_out)
                except asyncpg.exceptions.UniqueViolationError as e:
                    print(e)
            out_post = await get_ticker(conn=conn, table_name=fixings_table_name, ticker=ticker)
            print(out_post)
    finally:
        await pool.close()


if __name__ == '__main__':
    ticker_main = "AMZN"
    start = dt.date(2025, 1, 2)
    end = dt.date(2025, 8, 19)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(get_all_main())
    loop.run_until_complete(update(ticker=ticker_main, date_from=start, date_to=end))
    loop.run_until_complete(get_all_main())

