import pandas as pd
import sqlite3
import requests
import multiprocessing as mp
import os
from pprint import pprint
import pysolr
import itertools


key = os.environ['NYT_API']
template = "http://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key={api_key}"
nyt_df = pd.read_csv('nytimes_corps.csv')
corps = nyt_df.comnam.tolist()

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
	dic['date'] = json_item['pub_date']
	if json_item['snippet'] is not None:
		dic['snippet'] = json_item['snippet'].encode('utf-8')
	else:
		dic['snippet'] = 'None'
	dic['source'] = json_item['source']
	dic['url'] = json_item['web_url']
	return dic


'''Drop new data into dataframe by chunks - month by month'''
def update_database(dbfile, dbname, dataframe_sql_function, df):
	try:
		conn = sqlite3.connect(dbfile)
		conn.text_factory = str
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


def query_ids(json_rows, company_name):
	solr.delete(q='*:*')
	solr.add(json_rows)
	q = '(snippet: {company}) OR (headline: {company})'
	fq = {'fq': """(snippet:divest) 
				OR (snippet: divestment) 
				OR (snippet:divested) 
				OR (snippet:divestiture)
				OR (snippet:blacklist)
				OR (snippet:censor)
				OR (snippet: firesale)
				OR (snippet: fire sale)
				OR (snippet: dump)"""}
	results = solr.search(q.format(company=company_name), rows=100000, **fq)
	return [result['id'] for result in results]


def filter_rows(rows, ids):
	df = pd.DataFrame(rows)
	return df.loc[df['id'].isin(ids)]


if __name__ == '__main__':
	solr = pysolr.Solr('http://localhost:8983/solr/default/')
	for year in range(1962, 2019):
		for month in range(1, 13):
			url_df = pd.DataFrame()
			json = archives_request(template, year, month)
			pool = mp.Pool(processes=10)
			rows = [pool.apply(parse_json, args=(json_item,)) for json_item in json['response']['docs']]
			ids = [pool.apply(query_ids, args=(rows, corp)) for corp in corps]
			ids = list(itertools.chain(*ids))
			temp_df = filter_rows(rows, ids)
			url_df = url_df.append(temp_df, ignore_index=True)
	url_df.to_csv('nyt_url_data.csv', index=False)