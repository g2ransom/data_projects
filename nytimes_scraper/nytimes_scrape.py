import pandas as pd
from nytimesarticle import articleAPI
import time
import requests
from bs4 import BeautifulSoup
import re
from nltk.tokenize import sent_tokenize, word_tokenize

company_name = 'Apple Computer Inc.'

api = articleAPI("c412d5f33f9142c690134d02ee632940")

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


def company_search(company_name, indicator):
	search = api.search(q=company_name, begin_date='19620101', 
						end_date='20180402', fq={'source': 'The New York Times', 'body': indicator}, 
						fl=['web_url', 'pub_date'])
	rows = [create_dict(item, company_name) for item in search['response']['docs']]
	time.sleep(1)
	return pd.DataFrame() if not rows else pd.concat(rows, ignore_index=True) 

# for i in indicators:
# 	print(company_search(company_name, i, page=1), i)

# print(company_search(company_name, 'fire sale', page=1))

# dfs = [company_search(company_name, ind) for ind in indicators]
# dfs = map(lambda x: company_search(company_name, x), indicators)
# df = pd.concat(dfs, ignore_index=True)
# print(df.head())
# df.to_csv('nyt_sample.csv', index=False)


def get_url_text(url):
	r = requests.get(url)
	soup = BeautifulSoup(r.content, 'html5lib')
	all_text = soup.find_all('p')
	return [p.text.encode('utf-8') for p in all_text]


def split_text(raw_text):
	text = re.sub('[^a-z\ \']+', " ", raw_text)
	return list(text.split())


def count_indicators(word_list, indicators): return sum([1 for word in word_list if word in indicators])

# file = open('nyt_sample_1.txt', 'r')
# text = file.read().lower().decode('utf-8')
# file.close()


'''Get three sentences of sentences in text that include indicators'''
def get_indicator_text(text, indicators):
	sentences = sent_tokenize(text)
	c = [sentences[i:i+3] for i in range(len(sentences)-2)
		for word in word_tokenize(sentences[i]) if word in indicators]
	return c

# print(get_indicator_text(text, indicators))

# if __name__ == '__main__':





