import pandas as pd
from nytimesarticle import articleAPI
import time
import requests
from bs4 import BeautifulSoup
import re
from nltk.tokenize import sent_tokenize, word_tokenize
import os

company_name = 'Apple Computer Inc.'

key = os.environ['NYT_API']
api = articleAPI(key)

nyt_df = pd.read_csv('nytimes_corps.csv')
corps = nyt_df.comnam.tolist()
indicators = ['divest', 'divestment', 'divested', 'divestiture', 'blacklist', 'censor', 'firesale',
			'fire sale', 'dump']



def create_dict(search_item, company_name):
	dic = {}
	dic['url'] = search_item['web_url']
	dic['date'] = search_item['pub_date']
	dic['company'] = company_name
	row = pd.DataFrame(dic, index=[0])
	return row


def company_search(company_name, indicators, page=1):
	search = api.search(q=company_name, begin_date='19620101', 
						end_date='20180402', fq={'source': 'The New York Times', 'body': indicators}, 
						fl=['web_url', 'pub_date'], page=page)
	rows = [create_dict(item, company_name) for item in search['response']['docs']]
	time.sleep(1)
	return pd.DataFrame() if not rows else pd.concat(rows, ignore_index=True) 


def get_url_text(url):
	r = requests.get(url)
	soup = BeautifulSoup(r.content, 'html5lib')
	all_text = soup.find_all('p')
	return [p.text.encode('utf-8') for p in all_text]


def count_indicators(word_list, indicators): return sum([1 for word in word_list if word in indicators])


'''Get three sentences of sentences in text that include indicators'''
def get_indicator_text(text, indicators):
	sentences = sent_tokenize(text)
	c = [sentences[i:i+3] for i in range(len(sentences)-2)
		for word in word_tokenize(sentences[i]) if word in indicators]
	return c


def iterate_pages(company_name, indicators, end_range):
	df = pd.DataFrame()
	for i in range(1, end_range):
		rows = company_search(company_name, indicators, page=i)
		df = df.append(rows, ignore_index=True)
		if len(rows.index) < 10:
			return df


if __name__ == '__main__':
	df = iterate_pages(company_name, indicators, 50)
	urls = df.url.tolist()
	url_df = pd.DataFrame()
	for url in urls:
		page = url
		text = get_url_text(url)
		text = [w.decode('utf-8') for w in text]
		text = ' '.join(text)
		word_tokens = word_tokenize(text)
		indicator_count = count_indicators(word_tokens, indicators)
		indicator_text = get_indicator_text(text, indicators)
		temp_df = pd.DataFrame({'url': page, 'indicator_count': indicator_count, 'indicator_text': indicator_text})
		url_df = url_df.append(temp_df, ignore_index=True)
		# texts = url_df.apply(lambda x: pd.Series(x['indicator_text']),axis=1).stack().reset_index(level=1, drop=True)
		# texts.name = 'indicator_text'
		# url_df = url_df.drop('indicator_text', axis=1).join(texts)
	df = df.merge(url_df, how='left', on='url')
	print(df.head())





