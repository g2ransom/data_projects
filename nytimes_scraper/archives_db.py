import pandas as pd
import requests
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
	if json_item['abstract'] is not None:
		dic['abstract'] = json_item['abstract'].encode('utf-8')
	else:
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
def sql_dump(df):
	pass


if __name__ == '__main__':
	json = archives_request(template, 1851, 9)
	rows = [parse_json(json_item) for json_item in json['response']['docs']]
	df = pd.DataFrame(rows)
	print(df)


