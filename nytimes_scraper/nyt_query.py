import pandas as pd
import requests
import time
import multiprocessing as mp
import os
import itertools
import logging
import json
import psycopg2
from archives_db import psql_connect


indicators = ['divest', 'divestment', 'divested', 'divestiture', 'blacklist', 'censor', 'firesale',
			'fire sale', 'dump']
i = "|".join(indicators)

if __name__ == '__main__':
	with open('config.json', 'r') as f:
		conf = json.load(f)
	conn_str = "host={} dbname={} user={} password={}".format(conf['host'], conf['database'], conf['user'], conf['password'])		
	conn = psql_connect(conn_str)
	cur = conn.cursor()
	sql = """SELECT * FROM nytimes WHERE (headline ~* %s OR snippet ~* %s)"""
	cur.execute(sql, (i, i))
	nyt = pd.DataFrame(cur.fetchall())
	nyt.to_csv('nyt_filtered.csv', index=False)