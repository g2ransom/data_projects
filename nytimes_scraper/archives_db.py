import pandas as pd
import requests
import time
import multiprocessing as mp
import os
from pprint import pprint
import pysolr
import itertools
import logging
import json
import psycopg2
import sqlalchemy

key = os.environ['NYT_API']
template = "http://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"


def archives_request(template, year, month, key=key):
	url = template.format(year=year, month=month, api_key=key)
	try:
		response = requests.get(url, timeout=10)
		time.sleep(1)
		return response.json()
	except (requests.exceptions.RequestException, ValueError) as e:
		print(e, url)


def parse_json(json_item):
	dic = {}
	dic['id'] = json_item['_id']
	dic['headline'] = json_item['headline']['main'].encode('utf-8')
	dic['date'] = json_item['pub_date']
	try:
		if json_item['snippet'] is not None:
			dic['snippet'] = json_item['snippet'].encode('utf-8')
		else:
			dic['snippet'] = 'None'
	except KeyError, TypeError:
		dic['snippet'] = 'None'
	dic['url'] = json_item['web_url']
	return dic


def psql_connect(conn_str):
	try:
		conn = psycopg2.connect(conn_str)
		return conn
	except psycopg2.Error as e:
		conn.close()
		print("Connection Error: %s" % e)


def df_topsql(dbname, conn, df):
	df.to_sql(name=dbname, con=conn, if_exists="append", index=False)


if __name__ == '__main__':
	with open('config.json', 'r') as f:
		conf = json.load(f)
	conn_str = "host={} dbname={} user={} password={}".format(conf['host'], conf['database'], conf['user'], conf['password'])		
	conn = psql_connect(conn_str)
	cur = conn.cursor()
	pool = mp.Pool(processes=10)
	for year in range(1962, 2019):
		for month in range(1, 12):
			json = archives_request(template, year, month)
			try:
				rows = [pool.apply(parse_json, args=(json_item,)) for json_item in json['response']['docs']]
				values = [tuple(v for k,v in row.items()) for row in rows]
				print("Starting postgresql writing process.")
				args_str = ','.join(cur.mogrify("(%s, %s, %s, %s, %s)", x) for x in values)
				try:
					cur.execute("INSERT INTO nytimes (headline, date, snippet, url, id) VALUES " + args_str)
					print("Success for year %i month %i." % (year, month))
				except psycopg2.Error as e:
					print("This error occurred: %s" % e)
			except (TypeError, KeyError) as e:
				print("Type Error occurred for year %i month %i: %s" % (year, month, e))
			
	print("Completed database dump of NY Times archives data.")
	conn.close()
