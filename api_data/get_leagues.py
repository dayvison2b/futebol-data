import utils.firestore_handler as firestore_handler
from api_data_request import *

# Capturando as ligas brasileiras
league_data = make_request(base_url, 'leagues', '?country=Brazil', headers)
collection_name = 'leagues'
document_ids = firestore_handler.create_documents(collection_name, league_data)