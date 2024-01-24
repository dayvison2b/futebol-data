import pandas as pd
import utils.firestore_handler as firestore_handler
from utils.email_sender import send_email
from datetime import datetime, timedelta
import json
import time
from api_data.api_data_request import *

FIXTURES_LIMIT = 10

def get_fixtures(start_date, end_date):
    fixtures = firestore_handler.select_documents(
        collection_name='fixtures',
        conditions=[
            ("fixture.date", ">=", start_date),
            ("fixture.date", "<=", end_date)
        ],
        order_by= ("fixture.date", "ASCENDING"),
        limit=FIXTURES_LIMIT
    )
    return fixtures

def prediction_extract_info(fixtures_predictions):
    final_fixture_predictions = []
    for fixture_prediction in fixtures_predictions:
        try:
            fixture = fixture_prediction[0]
        except Exception as e:
            subject='Predictions Daily Update'
            message = f"""{e}

            {fixtures_predictions}

            {fixture_prediction}"""
            send_email(subject=subject, message=message)

        predictions_info = fixture['predictions']
        teams_info = fixture['teams']
        comparison_info = fixture['comparison']

        # Create a new data structure with only essential details
        final_fixture_predictions.append({
            'predictions': {
                'winner': predictions_info['winner'],
                'win_or_draw': predictions_info['win_or_draw'],
                'under_over': predictions_info['under_over'],
                'goals': predictions_info['goals'],
                'advice': predictions_info['advice'],
                'percent': predictions_info['percent']
            },
            'teams': teams_info,
            'comparison': {
                'form': comparison_info['form'],
                'att': comparison_info['att'],
                'def': comparison_info['def'],
                'poisson_distribution': comparison_info['poisson_distribution'],
                'h2h': comparison_info['h2h'],
                'goals': comparison_info['goals'],
                'total': comparison_info['total']
            }
        })
    return final_fixture_predictions

def odds_extract_info(fixtures_odds):
    final_fixture_odds = []
    for fixture_odds in fixtures_odds:
        try:
            fixture = fixture_odds[0]
        except Exception as e:
            subject='Predictions Daily Update'
            message = f"""{e}

            {fixtures_odds}

            {fixture_odds}"""
            send_email(subject=subject, message=message)

        bets = fixture['bookmakers'][0]['bets']
        update = fixture['update']

        # Create a new data structure with only essential details
        final_fixture_odds.append({
            'odds': bets[:2],
            'update': update,
            'bookmaker': fixture['bookmakers'][0]['name']
        })
    return final_fixture_odds

def clean_fixture_data(fixture):

    # Move 'goals' into 'teams'
    fixture['teams']['home']['goals'] = fixture['fixture'].get('goals', {}).get('home', None)
    fixture['teams']['away']['goals'] = fixture['fixture'].get('goals', {}).get('away', None)

    # Remove unnecessary information
    del fixture['goals']
    del fixture['score']
    del fixture['fixture']['status']
    del fixture['fixture']['periods']
    del fixture['league']['country']
    del fixture['league']['round']
    del fixture['league']['flag']
    del fixture['fixture']['venue']
    del fixture['fixture']['timestamp']

    return fixture

def process_predictions(fixture):
    try:
        prediction_available = True
        home_team = fixture['fixture']['teams']['home']['name']
        away_team = fixture['fixture']['teams']['away']['name']

        # Initialize result as unknown
        fixture['prediction']['predictions']['result'] = 'X'

        winner_name = fixture['prediction']['predictions']['winner']['name']
        if winner_name == home_team:
            fixture['prediction']['predictions']['winner']['venue'] = 'home'
            fixture['prediction']['predictions']['result'] = '1'
        elif winner_name == away_team:
            fixture['prediction']['predictions']['winner']['venue'] = 'away'
            fixture['prediction']['predictions']['result'] = '2'
        else:
            prediction_available = False
            fixture['prediction']['predictions']['winner']['venue'] = None
            fixture['prediction']['predictions']['result'] = None

        win_or_draw = fixture['prediction']['predictions']['win_or_draw']
        winner_venue = fixture['prediction']['predictions']['winner']['venue']
        if win_or_draw and winner_venue == 'home':
            fixture['prediction']['predictions']['result'] = '1X'
        elif win_or_draw and winner_venue == 'away':
            fixture['prediction']['predictions']['result'] = 'X2'
        else:
            fixture['prediction']['predictions']['result'] = None

        goals_home = fixture['prediction']['predictions']['goals']['home']
        goals_away = fixture['prediction']['predictions']['goals']['away']

        if prediction_available:
            if '-' in goals_home and '-' in goals_away:
                total_goals_home = float(goals_home.replace('-', '')) if goals_home else 0
                total_goals_away = float(goals_away.replace('-', '')) if goals_away else 0
                total_goals = total_goals_home + total_goals_away
                if total_goals >= 3:
                    fixture['prediction']['predictions']['goals_result'] = 'Under 3.5'
                else:
                    fixture['prediction']['predictions']['goals_result'] = 'Under 2.5'
            elif '+' in goals_home and '+' in goals_away:
                total_goals_home = float(goals_home.replace('+', '')) if goals_home else 0
                total_goals_away = float(goals_away.replace('+', '')) if goals_away else 0
                total_goals = total_goals_home + total_goals_away
                if total_goals > 2.5:
                    fixture['prediction']['predictions']['goals_result'] = 'Over 2.5'
                else:
                    fixture['prediction']['predictions']['goals_result'] = 'Over 1.5'
            else:
                fixture['prediction']['predictions']['goals_result'] = 'unknown'
        else:
            fixture['prediction']['predictions']['goals_result'] = 'unknown'

    except Exception as e:
        fixture_id = fixture['fixture']['fixture']['id']
        fixture_date = fixture['fixture']['fixture']['date']
        error_message = f"Error processing fixture {fixture_id} on {fixture_date}: {e}"
        raise Exception(error_message)

    return fixture

def check_last_predictions():
    yesterday = today - timedelta(days=1)

    # Get yesterday's predictions
    yesterday_predictions, yesterday_predictions_documents_id = firestore_handler.select_documents('predictions', [
        ('fixture.fixture.date', '>=', yesterday.strftime("%Y-%m-%dT")),
        ('fixture.fixture.date', '<', today.strftime("%Y-%m-%dT"))], receive_ids=True)

    for fixture, document_id in zip(yesterday_predictions, yesterday_predictions_documents_id):
        prediction = fixture['prediction']
        fixture = fixture['fixture']
        fixture_result = 'draw' if not fixture['teams']['home']['winner'] and not fixture['teams']['away']['winner'] else \
            'away' if fixture['teams']['away']['winner'] else 'home'

        predicted_winner = prediction['predictions']['winner']['venue']
        win_or_draw = prediction['predictions']['win_or_draw']

        update_data = {'result': None, 'update': today.strftime("%Y-%m-%dT%H:%M:%S+00:00")}

        if fixture_result == 'draw':
            if not win_or_draw:
                update_data['result'] = False
        else:
            if fixture_result == predicted_winner:
                update_data['result'] = True

            # Update the 'result' field in the predictions collection
        firestore_handler.update_document('predictions', document_id, update_data)
    return len(yesterday_predictions)

today = datetime.today()
start_date = today.strftime("%Y-%m-%dT")
end_date = (today + pd.DateOffset(days=25)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
next_fixtures = get_fixtures((today - timedelta(days=1)).strftime("%Y-%m-%dT"), end_date)

# Sort fixtures based on the 'date' field
next_fixtures = sorted(next_fixtures, key=lambda x: datetime.fromisoformat(x['fixture']['date']))

# Clean and treat next_fixtures
cleaned_next_fixtures = [clean_fixture_data(fixture) for fixture in next_fixtures]

fixtures_predictions = []
fixtures_odds = []
for fixture in next_fixtures:
    fixtures_predictions.append(make_request(base_url, 'predictions', f"?fixture={fixture['fixture']['id']}", headers))
    fixtures_odds.append(make_request(base_url, 'odds', f"?fixture={fixture['fixture']['id']}&bookmaker=8&timezone=America/Sao_Paulo", headers))
    #time.sleep(1)

fixtures_predictions = prediction_extract_info(fixtures_predictions)
fixtures_odds = odds_extract_info(fixtures_odds)

merged_data = []

for fixture, prediction, odd in zip(cleaned_next_fixtures, fixtures_predictions, fixtures_odds):
    merged_data.append({
        'fixture': fixture,
        'prediction': prediction,
        'odds': odd,
        'result': None,
        'update': None
    })

merged_data = [process_predictions(fixture) for fixture in merged_data]

today_str = today.strftime('%Y_%m_%d')

json_file_path = f'files/fixtures_{today_str}.json'
with open(json_file_path, 'w', encoding='utf-8') as json_file:
    json.dump(merged_data, json_file, ensure_ascii=False)
print("Fixtures exported")

def fix_empty_keys(data):
    if isinstance(data, list):
        return [fix_empty_keys(item) for item in data]
    elif isinstance(data, dict):
        for key, value in list(data.items()):
            if key == '':
                data['empty_key'] = data.pop(key)
            if isinstance(value, (list, dict)):
                fix_empty_keys(value)
    return data

merged_data_fixed = fix_empty_keys(merged_data)

collection_name = 'predictions'
deleted_documents = firestore_handler.delete_documents_by_where(collection_name, [('fixture.fixture.date', '>=', (today - timedelta(days=1)).strftime("%Y-%m-%dT"))])
document_ids = firestore_handler.create_documents(collection_name, merged_data_fixed)
predictions_updated = check_last_predictions()

if len(document_ids):
    subject = 'Predictions Daily Update'
    message = f"{len(document_ids)} new predictions have been uploaded."

    if not predictions_updated:
        message += "\nThere were no matches yesterday, so there are no fixtures to update with the results and check if our predictions were correct or incorrect."
    else:
        message += f"\n{predictions_updated} fixtures were updated with information indicating whether the predictions were correct or incorrect."
    send_email(subject=subject, message=message)

elif predictions_updated:
    subject = 'Predictions Daily Update'
    message = f"{predictions_updated} fixtures were updated with information indicating whether the predictions were correct or incorrect."
    message += "\nNo future fixture predictions were updated."
    send_email(subject=subject, message=message)

else:
    subject = 'Predictions Daily Update'
    message = 'No new predictions were uploaded, and there are no past fixtures to determine the accuracy of our predictions. This may be due to an error. Please check the predict.py script.'
    send_email(subject=subject, message=message)