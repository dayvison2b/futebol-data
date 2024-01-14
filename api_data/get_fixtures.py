import database
from api_data.api_data_request import *

# Capturando as partidas
important_leagues = [39,78,140,135,88,203,71]
collection_name = 'fixtures'
for league_id in important_leagues:
    fixtures_data = make_request(base_url, 'fixtures', f'?season=2023&league={league_id}&timezone=America/Sao_Paulo', headers)
    document_ids = database.create_documents(collection_name, fixtures_data)