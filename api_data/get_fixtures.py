from datetime import datetime, timedelta
from api_data_request import *

import time

from path_handler import add_parent_directory_to_path
add_parent_directory_to_path()

import utils.firestore_handler as firestore_handler
from utils.email_sender import send_email

def format_date(date):
    return date.strftime("%Y-%m-%d")

def get_fixtures_and_update(collection_name, leagues, date=None):
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
                for league_id in leagues:
                    fixtures_data = make_request(base_url, 'fixtures', f'?season=2023&league={league_id}&timezone=America/Sao_Paulo&from={start_date}&to={start_date}', headers)
                    time.sleep(12)
                    if fixtures_data:
                        document_ids = firestore_handler.create_documents(collection_name, fixtures_data)
                        documents_created += len(document_ids)

                if documents_created:
                    send_email("Daily Fixtures Update", f"{documents_created} fixtures from yesterday were updated!", recipient_emails)
                else:
                    send_email(f"Daily Fixtures Update", "There weren't fixtures for the date {start_date}.", recipient_emails)

            else:
                send_email("Daily Fixtures Update", "There weren't fixtures to delete for the date {start_date}.", recipient_emails)

        else:
            send_email("Daily Fixtures Update", "There weren't fixtures for the date {start_date}.", recipient_emails)

    except firestore_handler.DatabaseException as e:
        send_email("Daily Fixtures Update", str(e), recipient_emails)
        return

# Capturing fixtures
yesterday = datetime.today() - timedelta(days=1)
important_leagues = [39, 78, 140, 135, 88, 203, 71]
collection_name = 'fixtures'
daily_update = False

if not daily_update:
    for league_id in important_leagues:
        fixtures_data = make_request(base_url, 'fixtures', f'?season=2023&league={league_id}&timezone=America/Sao_Paulo', headers)
        document_ids = firestore_handler.create_documents(collection_name, fixtures_data)

else:
    get_fixtures_and_update(collection_name, important_leagues, date=yesterday)