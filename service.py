import datetime as dt
from aiohttp import web
from service_constants import port_number
from db_def import fixings_table_name
from db_stuff import get_ticker, get_date, get_entry, get_all, get_entry_attribute
from db_main import get_pool


async def handle_ticker(request):
    pool = request.app['pool']
    ticker_value = request.match_info.get('ticker', '')
    async with pool.acquire() as connection:
        rep = await get_ticker(conn=connection, table_name=fixings_table_name, ticker=ticker_value)
        rep_text = "\n".join([str(len(rep)), "\n".join(str(u) for u in rep)])
        return web.Response(text=rep_text)


async def handle_date(request):
    pool = request.app['pool']
    date_str = request.match_info.get('date', '')
    date_value = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    # print(date_value, type(date_value))
    async with pool.acquire() as connection:
        rep = await get_date(conn=connection, table_name=fixings_table_name, date=date_value)
        rep_text = "\n".join([str(len(rep)), "\n".join(str(u) for u in rep)])
        return web.Response(text=rep_text)


async def handle_entry(request):
    pool = request.app['pool']
    ticker_value = request.match_info.get('ticker', '')
    date_str = request.match_info.get('date', '')
    date_value = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    async with pool.acquire() as connection:
        rep = await get_entry(conn=connection, table_name=fixings_table_name, ticker=ticker_value, date=date_value)
        # rep_text = "\n".join([str(len(rep)), "\n".join(str(u) for u in rep)])
        rep_text_entry = "\n".join(f"{u}:\t{v}" for u, v in rep[0].items())
        return web.Response(text=rep_text_entry)


async def handle_entry_close(request):
    pool = request.app['pool']
    ticker_value = request.match_info.get('ticker', '')
    date_str = request.match_info.get('date', '')
    date_value = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
    async with pool.acquire() as connection:
        rep = await get_entry_attribute(
            conn=connection, table_name=fixings_table_name, ticker=ticker_value, date=date_value, attribute='close',
        )
        # rep_text = "\n".join([str(len(rep)), "\n".join(str(u) for u in rep)])
        rep_value = f"{rep[0]['close']:.2f}"
        return web.Response(text=rep_value)


async def handle_all(request):
    pool = request.app['pool']
    async with pool.acquire() as connection:
        rep = await get_all(conn=connection, table_name=fixings_table_name)
        rep_text = "\n".join([str(len(rep)), "\n".join(str(u) for u in rep)])
        return web.Response(text=rep_text)


async def create_db_pool(app_inst):
    app_inst['pool'] = await get_pool()

async def close_db_pool(app_inst):
    # Close the connection pool when the application shuts down
    app_inst['pool'].close()
    await app_inst['pool'].wait_closed()


def init_app():
    app_inst = web.Application()
    app_inst.on_startup.append(create_db_pool)
    app_inst.on_cleanup.append(close_db_pool)
    app_inst.router.add_route('GET', '/close/{ticker}/{date}', handle_entry_close)
    app_inst.router.add_route('GET', '/entry/{ticker}/{date}', handle_entry)
    app_inst.router.add_route('GET', '/ticker/{ticker}', handle_ticker)
    app_inst.router.add_route('GET', '/date/{date}', handle_date)
    app_inst.router.add_route('GET', '/all', handle_all)
    app_inst.router.add_route('GET', '/', handle_all)
    return app_inst


if __name__ == "__main__":
    app = init_app()
    web.run_app(app, port=port_number)

    # http://localhost:5000/ticker/SPY
    # http://localhost:5000/date/2025-08-08
    # http://localhost:5000/entry/SPY/2025-08-08
    # http://localhost:5000/close/SPY/2025-08-08