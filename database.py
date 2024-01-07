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

def select_documents(collection_name):
    collection_ref = db.collection(collection_name)
    
    docs = collection_ref.stream()
    
    documents_list = [doc.to_dict() for doc in docs]
    return documents_list

def select_documents_by_where(collection_name, *conditions):
    collection_ref = db.collection(collection_name)

    # Create a query with the initial condition
    query = collection_ref.where(*conditions[0])

    # Add additional conditions to the query
    for condition in conditions[1:]:
        query = query.where(*condition)

    # Execute the query and retrieve documents
    docs = query.stream()

    documents_list = [doc.to_dict() for doc in docs]
    return documents_list