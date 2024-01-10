import pandas as pd
import database
import requests

base_url = "https://v3.football.api-sports.io/"

with open('settings/api_key.txt', 'r') as api_key:
    api_key = api_key.read()

headers = {
  'x-rapidapi-key': api_key,
  'x-rapidapi-host': 'v3.football.api-sports.io'
}

endpoint = ''
parameters = ''

def get_teams(base_url, endpoint, parameters, headers):
    response = requests.request("GET", f'{base_url}{endpoint}{parameters}', headers=headers)
    return response

def get_leagues(base_url, endpoint, parameters, headers):
    response = requests.request("GET", f'{base_url}{endpoint}{parameters}', headers=headers)
    return response

def get_standings(base_url, endpoint, parameters, headers):
    response = requests.request("GET", f'{base_url}{endpoint}{parameters}', headers=headers)
    return response

def get_fixtures(base_url, endpoint, parameters, headers):
    response = requests.request("GET", f'{base_url}{endpoint}{parameters}', headers=headers)
    return response

def flatten_standings(standings_data):
    flattened_standings = []

    for league_info in standings_data:
        league = league_info['league']
        league_id = league['id']
        league_name = league['name']
        country = league['country']
        season = league['season']

        for standing_group in league['standings']:
            for team_info in standing_group:
                team_data = team_info['team']
                standing_data = team_info.copy()
                standing_data.pop('team')  # Remove nested team information
                standing_data.update({
                    'league_id': league_id,
                    'league_name': league_name,
                    'country': country,
                    'season': season,
                    'team_id': team_data['id'],
                    'team_name': team_data['name'],
                    'team_logo': team_data['logo'],
                })
                flattened_standings.append(standing_data)

    return flattened_standings

#-------------------------------------------------------------
#CAPTURANDO OS TIMES BRASILEIROS
teams_data = get_teams(base_url, endpoint='teams', parameters='?country=Brazil', headers=headers)
teams_data = teams_data.json()['response']

# Uploading all teams from league 71 and season 2023
collection_name = 'teams'
document_ids = database.create_documents(collection_name, teams_data)

#-------------------------------------------------------------
#CAPTURANDO AS LIGAS BRASILEIRAS
league_data = get_leagues(base_url=base_url, endpoint='leagues',parameters="?country=Brazil", headers=headers)
league_data = league_data.json()['response']

# Uploading all leagues from Brazil
collection_name = 'leagues'
document_ids = database.create_documents(collection_name, league_data)

#-------------------------------------------------------------
#CAPTURANDO AS CLASSIFICAÇÕES DAS SÉRIES MAIS IMPORTANTES DO BRASIL
for i in range(1, 7):
    standing_data = get_standings(base_url=base_url, endpoint='standings',parameters=f"?&season=2023&league=7{i}", headers=headers)
    standing_data = standing_data.json()['response']

    # Flatten the standing data
    flattened_standings = flatten_standings(standing_data)

    # Uploading all standings from league 71-76 and season 2023
    collection_name = 'standings'
    document_ids = database.create_documents(collection_name, flattened_standings)
    
 #-------------------------------------------------------------
#CAPTURANDO AS PARTIDAS
league_id = 71
fixtures_data = get_fixtures(base_url=base_url, endpoint='fixtures',parameters=f"?season=2023&league={league_id}&from=2023-01-01&to=2023-12-31&timezone=America/Sao_Paulo", headers=headers)
fixtures_data = fixtures_data.json()['response']

# Uploading all leagues from Brazil
collection_name = 'fixtures'
document_ids = database.create_documents(collection_name, fixtures_data)