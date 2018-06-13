import pandas as pd
from nytimesarticle import articleAPI
import time
import requests
from bs4 import BeautifulSoup
import re
from nltk.tokenize import sent_tokenize, word_tokenize
import os
import multiprocessing as mp


def get_url_text(url):
	try:
		r = requests.get(url)
		soup = BeautifulSoup(r.content, 'html5lib')
		all_text = soup.find_all('p')
		time.sleep(1)
		return [p.text.encode('utf-8') for p in all_text]
	except requests.exceptions.RequestException as e:
		print(e)


def count_indicators(word_list, indicators): return sum([1 for word in word_list if word in indicators])


def count_company(sentences, company_name): 
	sentences = sent_tokenize(text)
	return sum([1 for sentence in sentences if company_name in sentence])


def get_indicator_text(text, indicators):
	sentences = sent_tokenize(text)
	c = [sentences[i:i+3] for i in range(len(sentences)-2)
		for word in word_tokenize(sentences[i]) if word in indicators]
	return c

def get_company_text(text, company_name):
	sentences = sent_tokenize(text)
	c = [sentences[i:i+3] for i in range(len(sentences)-2) if company_name in sentences[i]]
	return c


if __name__ == '__main__':
	df = pd.read_csv('nyt_final061318.csv')
	urls = df.url.tolist()
	dates = df.created.tolist()
	corps = df.company.tolist()
	url_df = pd.DataFrame()
	for url, date, corp in zip(urls, dates, corps):
		text = get_url_text(url)
		text = [w.decode('utf-8') for w in text]
		text = ' '.join(text)
		word_tokens = word_tokenize(text)
		indicator_count = count_indicators(word_tokens, indicators)
		indicator_text = get_indicator_text(text, indicators)
		company_text = get_company_text(text, corp)
		company_count = count_company(text, corp)
		temp_df = pd.DataFrame({'url': url, 'date': date, 'company': corp, 'company_text': company_text, 
								'company_count':company_count, 'indicator_count': indicator_count, 
								'indicator_text': indicator_text})
		url_df = url_df.append(temp_df, ignore_index=True)
	print(url_df.head())
	url_df.to_csv('company_divest.csv', index=False)