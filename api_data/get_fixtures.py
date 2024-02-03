from datetime import datetime, timedelta
from api_data_request import *

from path_handler import add_parent_directory_to_path
add_parent_directory_to_path()

import utils.firestore_handler as firestore_handler
from utils.email_sender import send_email

def format_date(date):
    return date.strftime("%Y-%m-%d")

def get_fixtures_and_update(collection_name, date=None):
    if date is None:
        date = datetime.today() - timedelta(days=1)

    recipient_emails = ["dayvisoncordeiro2001@gmail.com", "gabrielmaia.amorim01@gmail.com", "felipegf600@gmail.com"]

    try:
        start_date = format_date(date)
        end_date = format_date(date + timedelta(days=1))

        fixture_ids = firestore_handler.select_documents(collection_name, conditions=[
            ('fixture.date', '>=', start_date),
            ('fixture.date', '<', end_date)
        ], receive_ids=True)[1]

        if fixture_ids:
            deleted_fixtures = firestore_handler.delete_documents_by_ids(collection_name, fixture_ids)

            if deleted_fixtures:
                documents_created = 0
                fixtures_data = make_request(base_url, 'fixtures', f"?date={start_date}&timezone=America/Sao_Paulo", headers)
                if fixtures_data:
                    document_ids = firestore_handler.create_documents(collection_name, fixtures_data)
                    documents_created += len(document_ids)

                if documents_created:
                    send_email("Daily Fixtures Update", f"{len(fixture_ids)} fixtures from yesterday were deleted! \n {documents_created} fixtures from yesterday were updated!", recipient_emails)
                else:
                    send_email("Daily Fixtures Update", f"There weren't fixtures for the date {start_date}. \n {len(fixture_ids)} fixtures from yesterday were deleted!", recipient_emails)

            else:
                send_email("Daily Fixtures Update", f"There weren't fixtures to delete for the date {start_date}.", recipient_emails)

        else:
            send_email("Daily Fixtures Update", f"There weren't fixtures for the date {start_date}.", recipient_emails)

    except firestore_handler.DatabaseException as e:
        send_email("Daily Fixtures Update", str(e), recipient_emails)
        return

# Capturing fixtures
yesterday = datetime.today() - timedelta(days=1)
collection_name = 'fixtures'

#Capturando todas as partidas do dia atual
fixtures_data = make_request(base_url, 'fixtures', f"?date={format_date(datetime.today())}&timezone=America/Sao_Paulo", headers)
document_ids = firestore_handler.create_documents(collection_name, fixtures_data)

# Atualizando partidas de ontem com os seus resultados
get_fixtures_and_update(collection_name, date=yesterday)