import utils.firestore_handler as firestore_handler
from api_data_request import *

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

# Capturando as classificações das séries mais importantes do Brasil
for i in range(1, 7):
    standing_data = make_request(base_url, 'standings', f'?&season=2023&league=7{i}', headers)
    flattened_standings = flatten_data(standing_data, i)
    collection_name = 'standings'
    document_ids = firestore_handler.create_documents(collection_name, flattened_standings)