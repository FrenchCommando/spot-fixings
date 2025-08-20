# Fixings

Daily fixings `Open/High/Low/Close/Volume`.


# Service Entrypoint

`localhost:5000`


# DB

postgresql

- Download and install the client.
- create the user: `fixings_user:fixings_pass`
- create the database `fixings`


# Infra

Run `service.bat` locally. (put it in startup folder if needed)

Alternatively, set up a docker-compose file (overkill for the current use).

This project has the full setup with `docker` (different topic): [uscis-github](https://github.com/FrenchCommando/uscis-status)


# Source

`thetadata`: need to run a `.jar` locally 

`yfinance`: everyone uses it although it's not perfect
