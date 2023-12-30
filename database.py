import firebase_admin
from firebase_admin import credentials, firestore

# Connect
cred = credentials.Certificate("settings/firebase-sdk.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def create_documents(collection_name, documents_data):
    """
    Create new documents in the specified Firestore collection.

    Parameters:
    - collection_name (str): The name of the Firestore collection.
    - documents_data (list): A list of dictionaries, where each dictionary represents the data for a document.

    Returns:
    - list: A list of document IDs for the newly created documents.
    """

    collection_ref = db.collection(collection_name)
    
    # List to store document IDs
    document_ids = []

    # Add documents to the collection
    for document_data in documents_data:
        try:
            # Add a new document with an auto-generated ID
            _, document_id = collection_ref.add(document_data)
            document_ids.append(document_id)
        except Exception as e:
            # Handle the exception (you might want to log it or take appropriate action)
            print(f"Error creating document: {e}")

    # Return the list of document IDs
    return document_ids