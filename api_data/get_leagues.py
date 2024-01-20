import utils.database as database
from api_data_request import *

# Capturando as ligas brasileiras
league_data = make_request(base_url, 'leagues', '?country=Brazil', headers)
collection_name = 'leagues'
document_ids = database.create_documents(collection_name, league_data)