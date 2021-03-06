#!/usr/bin/env python3

import sys
import argparse
import time
import datetime

import sqlite3
from urllib.request import urlopen, Request
import json
import dateutil.parser

DEFAULT_DB = 'bitso.sqlite'
DEFAULT_REQUEST_INTERNAL = 30
DEFAULT_BITSO_API_URL = 'https://api.bitso.com/v3/ticker/'

def update_database(url, db_cnx):
    try:
        res = urlopen(Request(url, headers={'User-Agent': 'Mozilla'}))
        ticker = res.read()
    except Exception as e:
        print(e)
        return

    ticker_json = json.loads(ticker.decode('utf-8'))

    stime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if ticker_json['success'] == True:
        c = db_cnx.cursor()
        for entry in ticker_json['payload']:
            c.execute('CREATE TABLE IF NOT EXISTS %s '
                      '(id integer PRIMARY KEY AUTOINCREMENT, created_at int, '
                      'volume real, ask real, low real, last real, bid real, '
                      'high real, vwap real);' % entry['book'])
            c.execute('CREATE INDEX IF NOT EXISTS %s_created_at_idx ON %s'
                      ' (created_at);' % (entry['book'], entry['book']))

            entry['created_at'] = int(dateutil.parser.parse(
                                      entry['created_at']).timestamp())
            c.execute('INSERT INTO %s (created_at, volume, ask, low, last, '
                      ' bid, high, vwap)'
                      'VALUES(%d, %s, %s, %s, %s, %s, %s, %s);' % \
                      (entry['book'], entry['created_at'], entry['volume'],
                       entry['ask'], entry['low'], entry['last'],
                       entry['bid'], entry['high'], entry['vwap']))

            print("%s: SUCCEED: %s" % (stime, str(entry)))
        db_cnx.commit()
    else:
        print("%s: FAILED" % stime)

def main():
    parser = argparse.ArgumentParser(
            description="Bitso market monitor",
            add_help=False)

    parser.add_argument('-s', '--server', default=DEFAULT_BITSO_API_URL,
            help='URL of Bitso, default: %s' % DEFAULT_BITSO_API_URL)
    parser.add_argument('-d', '--database', default=DEFAULT_DB,
            help='database to store market updates, default: %s' % DEFAULT_DB)
    parser.add_argument('-i', '--interval', default=DEFAULT_REQUEST_INTERNAL,
            type=int,
            help='interval in seconds to request market updates, '
                 'default: %dsec' % DEFAULT_REQUEST_INTERNAL)
    parser.add_argument('-h', '--help', action='help',
            default=argparse.SUPPRESS,
            help='show this help message and exit')

    args = parser.parse_args()

    db_cnx = sqlite3.connect(args.database)

    try:
        while True:
            update_database(args.server, db_cnx)
            time.sleep(args.interval)
    finally:
        db_cnx.close()

if __name__ == '__main__':
    try:
        ret =  main()
    except Exception:
        ret = 1
        import traceback
        traceback.print_exc()
    sys.exit(ret)
