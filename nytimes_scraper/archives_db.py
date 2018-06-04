import pandas as pd
import sqlite3
import requests
import multiprocessing as mp
import os
from pprint import pprint


key = os.environ['NYT_API']
template = "http://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"


def archives_request(template, year, month, key=key):
	url = template.format(year=year, month=month, api_key=key)
	try:
		response = requests.get(url, timeout=5)
		return response.json()
	except requests.exceptions.RequestException as e:
		print(e, url)


def parse_json(json_item):
	dic = {}
	dic['id'] = json_item['_id']
	try:
		if json_item['abstract'] is not None:
			dic['abstract'] = json_item['abstract'].encode('utf-8')
		else:
			dic['abstract'] = 'None'
	except KeyError:
			dic['abstract'] = 'None'
	dic['headline'] = json_item['headline']['main'].encode('utf-8')
	if json_item['keywords'] is not None:
		dic['keywords'] = [json_item['keywords'][i]['value'] for i in range(len(json_item['keywords']))]
	else:
		dic['keywords'] = 'None'
	dic['date'] = json_item['pub_date']
	if json_item['snippet'] is not None:
		dic['snippet'] = json_item['snippet']
	else:
		dic['snippet'] = 'None'
	dic['source'] = json_item['source']
	dic['url'] = json_item['web_url']
	return dic

'''Drop new data into dataframe by chunks - month by month'''
def update_database(dbfile, dbname, dataframe_sql_function, df):
	try:
		conn = sqlite3.connect(dbfile)
		cur = conn.cursor()
		return conn
	except Error:
		conn.close()
		print "Update Failure"
	finally:
		dataframe_sql_function(dbname, conn, df)
		print "Update Success!"
		conn.close()


def dataframe_tosql(dbname, conn, df):
	df.to_sql(name=dbname, con=conn, if_exists="append", index=False)


if __name__ == '__main__':
	json = archives_request(template, 2018, 1)
	pool = mp.Pool(processes=4)
	rows = [pool.apply(parse_json, args=(json_item,)) for json_item in json['response']['docs']]
	# rows = [parse_json(json_item) for json_item in json['response']['docs']]
	df = pd.DataFrame(rows)
	print(df)


