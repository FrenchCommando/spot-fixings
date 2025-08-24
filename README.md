# Fixings

Daily fixings `Open/High/Low/Close/Volume`.


# Service Entrypoint

`localhost:5000`

- use `data_push` to load and save data into `db`
- `entry` and `close` entrypoints queries the data from source if missing


# DB

postgresql

- Download and install the client.
- create the user: `fixings_user:fixings_pass`
- create the database `fixings`


# Infra

Run `service.bat`/`thetaservice.bat` locally. (put shortcut in startup folder if needed)

Alternatively, set up a `docker-compose` file (overkill for the current use).

This project has the full setup with `docker` (different topic): [uscis-github](https://github.com/FrenchCommando/uscis-status)


# Source

`thetadata`: need to run a `.jar` locally (EOD data is free - no need subscription) 

(deprecated) `yfinance`: everyone uses it, although it's not perfect
