# ccxt-ohlcv-fetcher

fetches historical OHLCV values from most crypto exchanges using ccxt library.
saves candles to a database (postgres/Mysql/Mariadb).
by default resumes from last candle fetched.


## setup

install virtualenv and python with your OS method

```
git clone https://github.com/planet-winter/ccxt-ohlcv-fetcher
cd ccxt-ohlcv-fetcher

virtualenv --python=python3.7 virtualenv
source virtualenv/bin/activate

pip install -r requirements.txt
```

install OS sqlite3

```
dnf install sqlite
```

## run

display help
```
./ccxt-ohlcv-fetch.py
```
get 1 min candles of XRP/USD data from bitfinex
```
./ccxt-ohlcv-fetch.py -s 'XRP/USD' -e bitfinex -t 1m --debug
```


## convert to freqtrade json

install dependencies into virtualenv

```
source virtualenv/bin/activate
pip install pandas
```

run conversion. creates folders per exchange and json files per pair and timeframe

```
./ccxt-kraken2json.py
```

in-place gzip all json files example. use your exchange name for exchange
```
gzip exchange/*.json
```
