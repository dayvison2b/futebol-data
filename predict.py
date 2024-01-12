import pandas as pd
import database
from datetime import datetime
import requests
import json
import time


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

def get_fixtures(date_init, to):
    # Assuming you want to filter fixtures based on the date range
    fixtures = database.select_documents_by_where(
        collection_name='fixtures',
        conditions=[
            ("fixture.date", ">=", date_init),
            ("fixture.date", "<=", to)
        ]
    )
    return fixtures

def extract_info(fixtures_predictions):
    final_fixture_predictions = []
    for fixture_prediction in fixtures_predictions:
        predictions_info = fixture_prediction[0]['predictions']
        league_info = fixture_prediction[0]['league']
        teams_info = fixture_prediction[0]['teams']
        comparison_info = fixture_prediction[0]['comparison']

        # Create a new data structure with only essential details
        final_fixture_predictions.append({
            'predictions': {
                'winner': predictions_info['winner'],
                'win_or_draw': predictions_info['win_or_draw'],
                'under_over': predictions_info['under_over'],
                'goals': predictions_info['goals'],
                'advice': predictions_info['advice'],
                'percent': predictions_info['percent']
            },
            'league': {
                'id': league_info['id'],
                'name': league_info['name'],
                'country': league_info['country'],
                'logo': league_info['logo'],
                'flag': league_info['flag'],
                'season': league_info['season']
            },
            'teams': {
                'home': {
                    'id': teams_info['home']['id'],
                    'name': teams_info['home']['name'],
                    'logo': teams_info['home']['logo']
                },
                'away': {
                    'id': teams_info['away']['id'],
                    'name': teams_info['away']['name'],
                    'logo': teams_info['away']['logo']
                }
            },
            'comparison': {
                'form': comparison_info['form'],
                'att': comparison_info['att'],
                'def': comparison_info['def'],
                'poisson_distribution': comparison_info['poisson_distribution'],
                'h2h': comparison_info['h2h'],
                'goals': comparison_info['goals'],
                'total': comparison_info['total']
            }
        })
    return final_fixture_predictions
    
today = datetime.today()
date_init = today.strftime("%Y-%m-%dT%H:%M:%S+00:00")
to = (today + pd.DateOffset(days=25)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
fixtures = get_fixtures(date_init, to)

# Sort fixtures based on the 'date' field
fixtures_sorted = sorted(fixtures, key=lambda x: datetime.fromisoformat(x['fixture']['date']))

# Select the next 25 fixtures
next_25_fixtures = fixtures_sorted[:10]
fixtures_predictions = []
# Example: Print the dates of the next 25 fixtures
for fixture in next_25_fixtures:
    #print(fixture['fixture']['date'])
    fixtures_predictions.append(make_request(base_url, 'predictions', f'?fixture={fixture['fixture']['id']}', headers))
    time.sleep(1)

fixtures_predictions = extract_info(fixtures_predictions)

today = today.strftime('%Y_%m_%d')

# Exporting the predictions
json_data = json.dumps(fixtures_predictions, indent=2, ensure_ascii=False)
json_file_path_predictions = f'fixtures_predictions_{today}.json'
with open(json_file_path_predictions, 'w') as json_file:
    json_file.write(json_data)
print(f"Predictions exported - {json_file_path_predictions}")

# Exporting the fixtures
json_data_fixtures = json.dumps(next_25_fixtures, indent=2, ensure_ascii=False)
json_file_path_fixtures = f'fixtures_{today}.json'
with open(json_file_path_fixtures, 'w') as json_file:
    json_file.write(json_data_fixtures)
print(f"Fixtures exported - {json_file_path_fixtures}")