import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# CONSTANTS
DELAY = 3
SEASON_RANGE = list(range(2022, 2020, -1))
COMP = "Premier-League" 
URL = f"https://fbref.com/en/comps/9/{COMP}-Stats"

def get_table(url, table_selector):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup.select(table_selector)[0]

def get_team_urls(url, table_selector):
    return [f"https://fbref.com{l.get('href')}" for l in get_table(url, table_selector).find_all('a') if '/squads/' in l.get('href')]

def get_data(url, match_text, get_data_func, season):
    data_list = []
    team_urls = get_team_urls(url, 'table.stats_table')
        
    for team_url in team_urls:
        response = requests.get(team_url)

        try:
            data = get_data_func(response, match_text)
            if match_text == 'Standard Stats':
                data.columns = data.columns.droplevel()
            
            team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
            data['Season'] = year
            data["Team"] = team_name
            data["Comp"] = COMP

            data_list.append(data)

        except Exception as e:
            print(f"Request for {team_url} failed. Error: {e}\nStatus Code: {response.status_code}")

        time.sleep(DELAY)

    return data_list

def html_table_to_dataframe(response, match_text):
    return pd.read_html(response.text, match=match_text)[0]

url = URL

for year in SEASON_RANGE:
    teams_players_data_table = get_data(url, "Standard Stats", html_table_to_dataframe, season=year)
    teams_data_table = get_data(url, "Scores & Fixtures", html_table_to_dataframe, season=year)

    soup = BeautifulSoup(requests.get(url).text)
    previous_season = soup.select("#meta > div:nth-child(2) > div")[0].find('a').get("href")
    url = f"https://fbref.com{previous_season}"