import datetime as dt
import httpx
import json



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
        d_line = json.loads(line)
        d = dict(
            Date=dt.datetime.strptime(d_line["created"], "%Y-%m-%dT%H:%M:%S.%f").date(),
            Open=d_line["open"], High=d_line["high"], Low=d_line["low"], Close=d_line["close"],
            Volume=d_line["volume"],
        )
        out_data.append(d)
    return out_data


def main():
    ticker_main = "AAPL"
    start_date = dt.date(2025, 8, 18)
    end_date = dt.date(2025, 8, 19)
    out_main = load_thetadata(ticker=ticker_main, date_from=start_date, date_to=end_date)
    print(out_main)


if __name__ == '__main__':
    main()
