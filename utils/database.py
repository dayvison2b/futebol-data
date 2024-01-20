import firebase_admin
from firebase_admin import credentials, firestore

# Connect
cred = credentials.Certificate("settings/firebase-sdk.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

def create_documents(collection_name, documents_data):
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

def select_documents_by_where(collection_name, conditions, order_by=None, limit=None, receive_ids=False):
    collection_ref = db.collection(collection_name)

    # Create a query with the initial condition
    query = collection_ref.where(*conditions[0])

    # Add additional conditions to the query
    for condition in conditions[1:]:
        query = query.where(*condition)

    if order_by is not None:
        query = query.order_by(order_by[0], direction=order_by[1])

    if limit is not None:
        query = query.limit(limit)

    # Execute the query and retrieve documents
    docs = query.stream()

    documents_list = []
    documents_ids = []

    for doc in docs:
        # Collect document data
        document_data = doc.to_dict()
        documents_list.append(document_data)

        # Collect document ID
        document_id = doc.id
        documents_ids.append(document_id)

    return (documents_list, documents_ids) if receive_ids else documents_list


def delete_documents_by_ids(collection_name, document_ids):
    collection_ref = db.collection(collection_name)

    # Iterate over each document ID and delete the corresponding document
    for document_id in document_ids:
        try:
            # Delete the document by ID
            collection_ref.document(document_id).delete()
            print(f"Document with ID {document_id} deleted successfully.")
        except Exception as e:
            raise Exception(f"Error deleting document with ID {document_id}: {e}")
    return True

def delete_documents_by_where(collection_name, conditions):
    collection_ref = db.collection(collection_name)

    # Create a query with the initial condition
    query = collection_ref.where(*conditions[0])

    # Add additional conditions to the query
    for condition in conditions[1:]:
        query = query.where(*condition)

    # Execute the query and retrieve documents
    docs = query.stream()

    # Iterate over each document and delete it
    for doc in docs:
        try:
            # Delete the document
            doc.reference.delete()
            print(f"Document with ID {doc.id} deleted successfully.")
        except Exception as e:
            raise Exception(f"Error deleting document with ID {doc.id}: {e}")
    return True



def update_document(collection_name, document_id, update_data):
    collection_ref = db.collection(collection_name).document(document_id)

    try:
        # Update the document with the provided data
        collection_ref.update(update_data)
        print(f"Document with ID {document_id} updated successfully.")
        return True
    except Exception as e:
        raise Exception(f"Error updating document with ID {document_id}: {e}")