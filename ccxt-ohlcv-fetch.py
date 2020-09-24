#!/usr/bin/env python3

__author__ = 'Daniel Winter'

import ccxt
import time
import math
import argparse
import signal
import sys
import os
import re
from ccxt.base.errors import NetworkError, DDoSProtection
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy import Column, BigInteger, String, Index
from sqlalchemy import desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError


DEFAULT_SINCE = "2013-01-01T00:00:00Z"
DEFAULT_SLEEP_SECONDS = 5*60
DEFAULT_RETRIES = 5
DEFAULT_MIN_BATCH_LEN = 24 * 60
EXTRA_RATE_LIMIT = 0


Base = declarative_base()


def get_candles_class(tablename):

    # http://sparrigan.github.io/sql/sqla/2016/01/03/dynamic-tables.html

    class CandlesTable(Base):

        __tablename__ = tablename

        timestamp = Column(BigInteger, primary_key=True)
        exchange = Column(String)
        symbol = Column(String)
        timeframe = Column(String)
        open = Column(String)
        high = Column(String)
        low = Column(String)
        close = Column(String)
        volume = Column(String)

    return CandlesTable


def persist_ohlcv_batch(ohlcv_batch, params):
    for ohlcv in ohlcv_batch:
        try:
            candle = params['table'](timestamp=int(ohlcv[0]),
                                     exchange=str(params['exchange']),
                                     symbol=str(params['symbol']),
                                     open=str(ohlcv[1]),
                                     high=str(ohlcv[2]),
                                     low=str(ohlcv[3]),
                                     close=str(ohlcv[4]),
                                     volume=str(ohlcv[5])  # convert to string here so 0.0 wont be casted to bool False
            )
        except TypeError as err:
            message("TypeError on Candle object creation %s, candle: %s" % (err, ohlcv), params, header="Error")
            sys.exit(127)
        try:
            params['dbsession'].add(candle)
            params['dbsession'].commit()
            #session.bulk_save_objects(candles, return_defaults=False,
            #    update_changed_only=True, preserve_order=True)
        except IntegrityError:
            message("Ignoring already fetched data, %s" % (ohlcv), params, header= "Info")
            if params['doquit']:
                sys.exit(0)
            else:
                time.sleep(DEFAULT_SLEEP_SECONDS)
        except OperationalError as err:
            message("An DB error happend: %s" % (err), params, header="Error")
            params['dbsession'].rollback()
            params['dbsession'].close()
            sys.exit(5)
        finally:
            params['dbsession'] .close()

        if params['debug']:
            print(str(params['exchange']), params['symbol'], params['exchange'].iso8601(ohlcv[0]), ohlcv[1:])


def get_last_candle_timestamp(params):
    last_timestamp = params['dbsession'].query(params['table']).order_by(desc(params['table'].timestamp)).limit(1).all()
    if last_timestamp != []:
        return int(last_timestamp[0].timestamp)
    else:
        return None


def get_ohlcv_batch(params):
    ohlcv_batch = []
    time.sleep(EXTRA_RATE_LIMIT)
    try:
        ohlcv_batch = params['exchange'].fetch_ohlcv(params['symbol'], params['timeframe'] , params['since'])
    except NetworkError:  # DDoSProtection et al
        message("NetworkError or DDoSprotection triggered: waiting for %s seconds" % (DEFAULT_SLEEP_SECONDS),
                params, header= "Info")
        time.sleep(DEFAULT_SLEEP_SECONDS)

    if ohlcv_batch is not None and len(ohlcv_batch):
        ohlcv_batch = ohlcv_batch[1:]
        return ohlcv_batch
    else:
        return None


def last_candle_is_incomplete(candle_timestamp, params):
    timeframe_re = re.compile(r'(?P<number>\d+)(?P<unit>[smhdwMy]{1})')
    match = timeframe_re.match(params['timeframe'])
    seconds = minutes = hours = days = weeks = months = years = 0
    lookup_dict = {'s': seconds, 'm': minutes, 'h': hours, 'd': days, 'w': weeks,
        'M': months, 'y': years}\

    if match is not None:
        matchdict = match.groupdict()
        lookup_dict[matchdict['unit']] = int(matchdict['number'])
        candle_dt = datetime.fromtimestamp(candle_timestamp / 1000)
        exchange_dt = datetime.fromtimestamp(params['exchange'].milliseconds() / 1000)
        # eg. timeframe=1d and candle_timestamp=2019-01-01T00:00:00Z
        #  exchange_dt=2019-01-02T01:00:00Z
        #
        #  2019-01-02T01:00:00Z - 1 day = 2019-01-01T00:00:00Z
        # use relativetimedelta as included batteries don't offer years
        #  or months
        one_candle_delta = relativedelta(years=lookup_dict['y'],
            months=lookup_dict['M'], weeks=lookup_dict['w'],
            days=lookup_dict['d'], hours=lookup_dict['h'],
            minutes=lookup_dict['m'], seconds=lookup_dict['s'])
        return exchange_dt - one_candle_delta < candle_dt

    else:
        message("Could not parse timeframe %s" % candle_timeframe, params, header="Error")

        
def message(msg, params, header=None):
    print(header.center(80, '-'))
    if params is not None:
        print(str(params['exchange']), params['symbol'], msg)
    else:
        print(msg)
    print('-'*80)

    
def parse_args():
    parser = argparse.ArgumentParser(description='CCXT Market Data Downloader')

    parser.add_argument('-s', '--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair To Download')

    parser.add_argument('-e', '--exchange',
                        type=str,
                        required=True,
                        help='The exchange to download from')

    parser.add_argument('-t', '--timeframe',
                        type=str,
                        help='The timeframe to download. examples: 1m, 5m, \
                                15m, 30m, 1h, 2h, 3h, 4h, 6h, 12h, 1d, 1M, 1y')

    parser.add_argument('--since',
                        type=str,
                        help='The iso 8601 starting fetch date. Eg. 2018-01-01T00:00:00Z')

    parser.add_argument('--debug',
                        action = 'store_true',
                        help=('Print Sizer Debugs'))

    parser.add_argument('-r', '--rate-limit',
                        type=int,
                        help='eg. 60 to wait for one minute more')

    parser.add_argument('-q', '--quit',
                        action = 'store_true',
                        help='exit program after fetching latest candle')


    return parser.parse_args()


def check_args(args):
    params = {}

    # Get our Exchange
    try:
        params['exchange'] = getattr(ccxt, args.exchange)({
           'enableRateLimit': True,
        })
    except AttributeError:
        message('Exchange "{}" not found. Please check the exchange \
                is supported.'.format(args.exchange), None, header='Error')
        sys.exit(1)

    if args.rate_limit:
       EXTRA_RATE_LIMIT = args.rate_limit

    # Check if fetching of OHLC Data is supported
    if params['exchange'].has["fetchOHLCV"] == False:
        message('{} does not support fetching OHLCV data. Please use \
            another exchange'.format(args.exchange), None, header='Error')
        sys.exit(1)
        
    if params['exchange'].has['fetchOHLCV'] == 'emulated':
        message('{} uses emulated OHLCV. This script does not support \
                this'.format(args.exchange), None, header='Error')
        sys.exit(1)

    # Check requested timeframe is available. If not return a helpful error.
    if args.timeframe not in params['exchange'].timeframes:
        message('The requested timeframe ({}) is not available from {}\n\
                Available timeframes are:\n{}'.format(args.timeframe,
                args.exchange, ''.join(['  -' + key + '\n' for key in
                params['exchange'].timeframes.keys()])), None, header='Error')
        sys.exit(1)
    else:
        params['timeframe'] = args.timeframe

    # Check if the symbol is available on the Exchange
    params['exchange'].load_markets()
    if args.symbol not in params['exchange'].symbols:
        message('The requested symbol {} is not available from {}\n'
                'Available symbols are:\n{}'.format(args.symbol, args.exchange,
                ''.join(['  -' + key + '\n' for key in params['exchange'].symbols])),
                None, header='Error')
        sys.exit(1)
    else:
        params['symbol'] = args.symbol


    engine = create_engine("postgresql://kraken:ReleaseTheKraken@127.0.0.1/kraken")
    # create database object with generated table name
    tablename = '%s_%s_%s' % (args.exchange, args.symbol, args.timeframe)
    CandlesTable = get_candles_class(tablename)
    params['table'] = CandlesTable
    Base.metadata.create_all(engine)

    Session = sessionmaker()
    Session.configure(bind=engine)
    params['dbsession'] = Session()

    since = None
    if not args.since:
        params['since'] = get_last_candle_timestamp(params)
        if params['since'] is None:
            params['since'] = params['exchange'].parse8601(DEFAULT_SINCE)
            message('Starting with default since value of {}.'.format(DEFAULT_SINCE), None, header='Info')

        else:
            if args.debug:
                message('resuming from last db entry {}'.format(params['exchange'].iso8601(params['since'])),
                        None, header='Info')
    else:
        params['since'] = params['exchange'].parse8601(args.since)
        if params['since'] is None:
            message('Could not parse --since. Use format 2018-12-24T00:00:00Z', None,
                    header='Error')
            sys.exit(22)

    if not params['exchange'].has['fetchOHLCV']:
        message('Exchange "{}" has no method fetchOHLCV.'.format(args.exchange), None, header='Error')
        sys.exit(95)

    params['debug'] = args.debug
    params['doquit'] = args.quit

    return params


def main():
    args = parse_args()
    params = check_args(args)

    exchange_milliseconds = params['exchange'] .milliseconds()
    while True:
        ohlcv_batch = get_ohlcv_batch(params)
        if ohlcv_batch is not None and len(ohlcv_batch):
            # some exchanges return data in other order
            ohlcv_batch_ascending = sorted(ohlcv_batch, key=lambda ohlcv: ohlcv)
            last_candle = ohlcv_batch_ascending[-1]
            last_candle_timestamp = last_candle[0]
            if last_candle_is_incomplete(last_candle_timestamp, params):
                # delete last incomplete candle from list
                del ohlcv_batch_ascending[-1]
                persist_ohlcv_batch(ohlcv_batch_ascending, params)
                message('last candle incomplete: dropped it', params, header="Info")
                if params['doquit']:
                    message('Up to date. Stopping', params, header='Info')
                    sys.exit(0)
                else:
                    params['since'] = ohlcv_batch_ascending[-1][0]
                    time.sleep(DEFAULT_SLEEP_SECONDS)
            else:
                params['since'] = ohlcv_batch_ascending[-1][0]
                persist_ohlcv_batch(ohlcv_batch_ascending, params)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        message("Fetcher finished by user", None, header='ERROR')
    except Exception as err:
        message("Fetcher failed with exception\n {}".format(err), None, header='ERROR')
        raise
