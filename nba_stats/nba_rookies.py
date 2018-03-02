import requests
from bs4 import BeautifulSoup
import pandas as pd
from nba_py import player

def create_draft_data(start, end):
	template = "https://www.basketball-reference.com/draft/NBA_{year}.html"
	soup = create_soup(template, 'html5lib', 2017)
	column_headers = create_headers(soup)
	
	draft_df = pd.DataFrame()
	for year in range(start, end+1):
		soup = create_soup(template, 'html5lib', year)
		rows = soup.find_all('tr', limit=22)[2:]
		draft_data = [[td.get_text() for td in rows[i].find_all('td')] 
						for i in range(len(rows))]
		year_df = pd.DataFrame(draft_data, columns=column_headers)
		year_df.insert(0, 'DraftYr', year)
		draft_df = draft_df.append(year_df, ignore_index=True)
	return draft_df


def clean_ids(df):
    indices = [18, 102, 109, 133, 154, 176, 188, 197]
    ids = [201581, 203490, 203468, 203933, 1626162, 1627735, 1628372, 1628388]
    
    for index, player_id in zip(indices, ids):
        df.iloc[index, df.columns.get_loc('IDs')] = player_id
    return df


def create_soup(template, parser, year):
	url = template.format(year=year)
	r = requests.get(url)
	soup = BeautifulSoup(r.content, parser)
	return soup


def create_headers(soup):
	headers = [th.get_text() for th in soup.find_all('tr', limit=2)[1].find_all('th')]
	return headers[1:]


def add_ids(df):
	df.DraftYr = df.DraftYr.apply(create_season_string)
	players = df.Player.tolist()
	years = df.DraftYr.tolist()
	id_lists = []
	for player_name, year in zip(players, years):
		name_split = player_name.split()
		first = name_split[0]
		last = name_split[-1]
		year = year
		id_lists.append([first, last, year])
	
	ids = []
	for i in range(len(id_lists)):
		try:
			player_id = player.get_player(id_lists[i][0], last_name=id_lists[i][1], season=id_lists[i][2])
			ids.append(player_id)
		except player.PlayerNotFoundException:
			ids.append(0)
	rookie_ids = map(get_values, ids)
	rookie_series = pd.Series(rookie_ids)
	df['IDs'] = rookie_series.values
	return df
	

def get_values(n):
	return n.values[0] if isinstance(n, pd.Series) else 0


def create_season_string(season):
	return str(season) + '-' + str(season + 1)[2:]


def create_rookie_data(nba_id_list):
    rookie_df = pd.DataFrame()
    rookie_data = map(extract_data, nba_id_list)
    rookie_df = rookie_df.append(rookie_data, ignore_index=True)
    return rookie_df


def extract_data(player_id):
	player_object = player.PlayerYearOverYearSplits(player_id)
	player_data = player_object.by_year()
	rookie_data = player_data.tail(n=1)
    return rookie_data


draft_df = create_draft_data(2008, 2017)
draft_df = add_ids(draft_df)
draft_df = clean_ids(draft_df)
draft_df = draft_df.rename(columns={'PTS': 'PTS_TOTAL'})
ids = draft_df.IDs.tolist()
rookie_df = create_rookie_data(ids)
df = pd.concat([draft_df, rookie_df], axis=1)
df.to_csv("nba_stats.csv", index=False)

