import utils.database as database
from datetime import datetime
import json
import time
from api_data_request import *

def get_fixtures(date_init):
    fixtures = database.select_documents_by_where(
        collection_name='fixtures',
        conditions=[
            ("fixture.date", "<=", date_init)
        ]
    )
    return fixtures
    
today = datetime.today()
date_init = today.strftime("%Y-%m-%dT%H:%M:%S+00:00")
fixtures = get_fixtures(date_init)
fixtures_sorted = sorted(fixtures, key=lambda x: datetime.fromisoformat(x['fixture']['date']))
fixtures = fixtures_sorted[:5]

for fixture in fixtures:
    home_team_id = fixture['teams']['home']['id']
    away_team_id = fixture['teams']['away']['id']
    
    home_statistics = make_request(base_url, 'fixtures/statistics', f'?fixture={fixture['fixture']['id']}&team={home_team_id}', headers)
    away_statistics = make_request(base_url, 'fixtures/statistics', f'?fixture={fixture['fixture']['id']}&team={away_team_id}', headers)
    
    # Update the fixture dictionary with statistics
    fixture.update({
        'home_statistics': home_statistics,
        'away_statistics': away_statistics
    })
    
    time.sleep(1)
    
today = today.strftime('%Y_%m_%d')

# Export the updated fixtures to a JSON file
with open('fixtures_to_predict.json', 'w') as json_file:
    json.dump(fixtures, json_file, indent=2, ensure_ascii=False)
print(f"Fixtures to predict exported")