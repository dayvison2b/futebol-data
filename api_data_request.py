import pandas as pd
import database
import requests

base_url = "https://v3.football.api-sports.io/"

with open('settings/api_key.txt', 'r') as api_key_file:
    api_key = api_key_file.read()

headers = {
    'x-rapidapi-key': api_key,
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

def make_request(base_url, endpoint, parameters, headers):
    response = requests.get(f'{base_url}{endpoint}{parameters}', headers=headers)
    return response.json().get('response', {})

def flatten_data(data, league_id):
    flattened_data = []

    for league_info in data:
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
                flattened_data.append(standing_data)

    return flattened_data

# Capturando os times brasileiros
teams_data = make_request(base_url, 'teams', '?country=Brazil', headers)
collection_name = 'teams'
document_ids = database.create_documents(collection_name, teams_data)

# Capturando as ligas brasileiras
league_data = make_request(base_url, 'leagues', '?country=Brazil', headers)
collection_name = 'leagues'
document_ids = database.create_documents(collection_name, league_data)

# Capturando as classificações das séries mais importantes do Brasil
for i in range(1, 7):
    standing_data = make_request(base_url, 'standings', f'?&season=2023&league=7{i}', headers)
    flattened_standings = flatten_data(standing_data, i)
    collection_name = 'standings'
    document_ids = database.create_documents(collection_name, flattened_standings)

# Capturando as partidas

important_leagues = [39,78,140,135,88,203,71]
collection_name = 'fixtures'
for league_id in important_leagues:
    fixtures_data = make_request(base_url, 'fixtures', f'?season=2023&league={league_id}&timezone=America/Sao_Paulo', headers)
    document_ids = database.create_documents(collection_name, fixtures_data)
