import utils.database as database
from api_data_request import *

# Capturando os times brasileiros
teams_data = make_request(base_url, 'teams', '?country=Brazil', headers)
collection_name = 'teams'
document_ids = database.create_documents(collection_name, teams_data)
