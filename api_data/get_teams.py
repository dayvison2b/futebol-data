import utils.firestore_handler as firestore_handler
from api_data_request import *

# Capturando os times brasileiros
teams_data = make_request(base_url, 'teams', '?country=Brazil', headers)
collection_name = 'teams'
document_ids = firestore_handler.create_documents(collection_name, teams_data)
