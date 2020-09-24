#!/usr/bin/env python3
import sys
import os
import json
import gzip
import re
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy.engine import reflection
from pathlib import Path


def main():
    engine = create_engine("postgresql://kraken:ReleaseTheKraken@127.0.0.1/kraken")
    Session = sessionmaker(bind=engine)
    session = Session()

    with engine.connect() as connection:
        insp = reflection.Inspector.from_engine(engine)
        tables = insp.get_table_names()
        metadata = MetaData()

        print(tables)

        for table in tables:
            print("testing: ", table)
            table_re = re.compile('^(?P<exchange>\w[^_]+)_(?P<currency>\w[^_]+)/(?P<quote_currency>\w[^_]+)_(?P<timeframe>\w[^_]+)$')
            table_groups = re.search(table_re, table)
            if table_groups is None:
                continue

            print("converting table: ", table)

            ohlcv = Table(table, metadata, autoload=True, autoload_with=engine)

            df = pd.read_sql_table(
                table,
                con=engine,
                schema='public',
                index_col='timestamp',
                coerce_float=True,
                columns=[
                  'timestamp',
                  'open',
                  'high',
                  'low',
                  'close',
                  'volume'
                ],
            )

            export_json(df,
                        exchange=table_groups["exchange"],
                        currency=table_groups["currency"],
                        quote_currency=table_groups["quote_currency"],
                        timeframe=table_groups["timeframe"]
            )


def export_json(df, exchange=None, currency=None, quote_currency=None, timeframe=None):
    filename = "%s_%s-%s.json" % (currency, quote_currency, timeframe)
    Path(exchange).mkdir(exist_ok=True)
    full_filename = "%s%s%s" % (exchange, os.path.sep, filename)

    df.to_json(full_filename, orient='values')




if __name__ == "__main__":
    main()
