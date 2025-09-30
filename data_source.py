import datetime as dt
import httpx
import json
import yfinance as yf



BASE_URL_v3 = "http://localhost:25503/v3"
date_format = "%Y%m%d"


def load_thetadata(ticker, date_from, date_to):
    # http://localhost:25503/v3/stock/history/eod?symbol=AAPL&start_date=20240101&end_date=20240131
    # http://localhost:25503/v3/stock/history/eod?symbol=AAPL&start_date=20240101&end_date=20240131&format=json
    url = BASE_URL_v3 + '/stock/history/eod'

    if ticker in ["SPX", "VIX", "RUT", "DJX"]:
        # http://localhost:25503/v3/index/history/eod?symbol=DJX&start_date=20250808&end_date=20250808&format=json
        url = BASE_URL_v3 + '/index/history/eod'  # no volume

    params = {
        'symbol': f'{ticker}',
        'start_date': f'{date_from:{date_format}}', 'end_date': f'{date_to:{date_format}}',
        'format': 'ndjson',
    }

    response = httpx.get(url, params=params, timeout=60)  # make the request
    response.raise_for_status()  # make sure the request worked

    data = response.text
    out_data = []
    for line in data.splitlines():
        if not line:
            continue
        d_line = json.loads(line)
        # print(d_line)
        d = dict(
            Date=dt.datetime.strptime(d_line["created"].split(".", 1)[0], "%Y-%m-%dT%H:%M:%S").date(),
            Open=d_line["open"], High=d_line["high"], Low=d_line["low"], Close=d_line["close"],
            Volume=d_line["volume"],
        )
        # print(d)
        out_data.append(d)
    return out_data


yf_mapping = dict(
    NDX="^NDX",
)  # mapping means override - don't add SPX


def load_yf(ticker, date_from, date_to):
    yf_ticker = yf_mapping.get(ticker, ticker)
    ticker_obj = yf.Ticker(yf_ticker)
    historical_data = ticker_obj.history(start=date_from, end=date_to + dt.timedelta(days=1))
    out_data = []
    for line in historical_data.iterrows():
        d_date, d_line = line
        d = dict(
            Date=d_date.date(),
            Open=d_line["Open"], High=d_line["High"], Low=d_line["Low"], Close=d_line["Close"],
            Volume=d_line["Volume"],
        )
        out_data.append(d)

    return out_data


def load_fixings(ticker, date_from, date_to):
    if ticker in yf_mapping:
        return load_yf(ticker=ticker, date_from=date_from, date_to=date_to)
    else:
        return load_thetadata(ticker=ticker, date_from=date_from, date_to=date_to)


def main():
    ticker_main = "SPX"
    start_date = dt.date(2024, 2, 7)
    end_date = dt.date(2024, 2, 10)
    out_main = load_thetadata(ticker=ticker_main, date_from=start_date, date_to=end_date)
    out_main_yf = load_yf(ticker=ticker_main, date_from=start_date, date_to=end_date)
    out_main0 = load_fixings(ticker=ticker_main, date_from=start_date, date_to=end_date)
    print(out_main)
    print(out_main_yf)
    print(out_main0)

    # ERROR: Internal server error: Too many days between start and end date; max 365 days allowed
    # ERROR: Internal server error: EOD is not available for the current day


if __name__ == '__main__':
    main()
