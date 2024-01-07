import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import warnings
import hashlib

# Suprimindo os warnings temporariamente
warnings.filterwarnings("ignore", category=FutureWarning)

# CONSTANTS
DELAY = 3
SEASON_RANGE = list(range(2023, 2020, -1))
COMP = "Premier-League" # IMPORTANTE (A URL ABAIXO ESTÁ DINAMICAMENTE PEGANDO ESSA CONSTANTE)
URL = f"https://fbref.com/en/comps/9/{COMP}-Stats" # URL constante (usada como base) da premier league, alterar para o Brasileirão, se necessário

url = URL # url não constante, pois será alterada ao longo do fluxo

def create_player_id(row):
    player_name = row['Player']
    age = row['Age']
    nation = row['Nation']

    # Concatenar informações únicas
    player_info = f"{player_name}_{age}_{nation}"

    # Calcular o hash MD5 para criar um ID único
    player_id = hashlib.md5(player_info.encode()).hexdigest()

    return player_id

def create_team_id(row):
    team_name = row['Team']

    # Concatenar informações únicas
    team_info = f"{team_name}"

    # Calcular o hash MD5 para criar um ID único
    player_id = hashlib.md5(team_info.encode()).hexdigest()

    return player_id

def get_team_data(team_url, feature_match, create_id_func, columns_to_drop, year, type):
    response = requests.get(team_url)

    try:
        team_data = pd.read_html(response.text, match=feature_match)[0]
        team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
        season = team_url.split("/")[-2][:9] if year != 2023 else '2022-2023'

        if type == "player":
            team_data.columns = team_data.columns.droplevel()
        team_data = team_data.loc[:, ~team_data.columns.duplicated(keep='first')]
        
        for coluna in columns_to_drop:
            if coluna in team_data.columns:
                team_data.drop(columns=coluna, inplace=True)

        team_data["Season"] = season
        team_data["Team"] = team_name
        team_data[type + "_id"] = team_data.apply(create_id_func, axis=1)

        return team_data

    except Exception as e:
        print(f"""A solicitação para {team_url} falhou.
              Erro: {e} \n
              código de status: {response.status_code}""")
        return None

teams_players_data_table = []
players_table = []
players_performance = []
teams_data_table = []
matches_table = []

for year in SEASON_RANGE:
    data = requests.get(url)
    soup = BeautifulSoup(data.text, features='lxml')
    table = soup.select('table.stats_table')[0]

    links = [l.get("href") for l in table.find_all('a')]
    links = [l for l in links if '/squads/' in l]
    team_urls = [f"https://fbref.com{l}" for l in links]

    previous_season = soup.select("#meta > div:nth-child(2) > div")[0].find('a').get("href")
    url = f"https://fbref.com{previous_season}"

    feature_match_players = "Standard Stats"
    columns_to_drop_players = ['xG', 'npxG', 'xAG', 'npxG+xAG', 'G+A-PK', 'Matches','xG+xAG']
    
    feature_match_teams = "Scores & Fixtures"
    columns_to_drop_teams = ['xG', 'xGA', 'Age','Notes','Match Report']

    for team_url in team_urls:
        # Obter dados dos jogadores
        team_player_data = get_team_data(team_url, feature_match_players, create_player_id, columns_to_drop_players, year, "player")
        if team_player_data is not None:
            teams_players_data_table.append(team_player_data)
            players_performance.append(team_player_data.loc[:, ~team_player_data.columns.isin(['Player', 'Nation', 'Age'])])
            players_table.append(team_player_data[['player_id','Player','Nation','Age']])
            
        time.sleep(DELAY)

        # Obter dados das equipes
        team_data = get_team_data(team_url, feature_match_teams, create_team_id, columns_to_drop_teams, year, "team")
        if team_data is not None:
            teams_data_table.append(team_data)
            matches_table.append(team_data.loc[:, ~team_data.columns.isin(['xG', 'xGA', 'Age','Notes','Match Report'])].rename(columns={"GF":"Goals for", "GA":"Goals against"}))

        time.sleep(DELAY)

# ***PARA SALVAR AS TABELAS***

pd.set_option('display.max_columns', None)

dict_players_performance = [df.to_dict(orient="records") for df in players_performance]
dict_players_table = [df.to_dict(orient="records") for df in players_table]
dict_matches_table = [df.to_dict(orient="records") for df in matches_table]

# Importar e salvar dados no banco de dados
from database import create_documents

for player_performance in dict_players_performance:
    documents = create_documents('players_performance', player_performance)

for player in dict_players_table:
    documents = create_documents('players', player)

for match in dict_matches_table:
    documents = create_documents('matches', match)
