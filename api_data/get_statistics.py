from datetime import datetime, timedelta
import time

import pandas as pd

from path_handler import add_parent_directory_to_path
add_parent_directory_to_path()

from api_data_request import make_request, base_url, headers
from utils.firestore_handler import select_documents

def process_statistics(stats_list):
    # Initialize an empty dictionary to store processed statistics
    processed_stats = {}
    
    # Iterate through each dictionary in the list
    for stat in stats_list:
        # Extract 'type' and 'value' from each dictionary
        stat_type = stat['type']
        stat_value = stat['value']
        
        # Store the information in the processed_stats dictionary
        processed_stats[stat_type] = stat_value
    
    return processed_stats


def get_fixtures(date_init):
    fixtures = select_documents(
        collection_name='fixtures',
        conditions=[
            ("fixture.date", "<=", date_init)
        ]
    )
    return fixtures

 
today = datetime.today() - timedelta(days=1)
date_init = today.strftime("%Y-%m-%dT%H:%M:%S+00:00")
fixtures = get_fixtures(date_init)
fixtures_sorted = sorted(fixtures, key=lambda x: datetime.fromisoformat(x['fixture']['date']), reverse=True)
fixtures = fixtures_sorted[:5]

for fixture in fixtures:
    home_team_id = fixture['teams']['home']['id']
    away_team_id = fixture['teams']['away']['id']
    
    home_statistics = make_request(base_url, 'fixtures/statistics', f'?fixture={fixture['fixture']['id']}&team={home_team_id}', headers)
    away_statistics = make_request(base_url, 'fixtures/statistics', f'?fixture={fixture['fixture']['id']}&team={away_team_id}', headers)

    # Update the fixture dictionary with statistics
    fixture.update({
        'home_statistics': home_statistics,
        'away_statistics': away_statistics
    })
    
    time.sleep(1)
    
df = pd.json_normalize(fixtures)

df_home_stats = pd.json_normalize(df['home_statistics'])
df_home_stats.columns = ['home_statistics']
df_away_stats = pd.json_normalize(df['away_statistics'])
df_away_stats.columns = ['away_statistics']

df = df.drop(['home_statistics', 'away_statistics'], axis=1)
df = pd.concat([df, df_home_stats, df_away_stats], axis=1)

df_home_stats = pd.json_normalize(df['home_statistics'])
df_home_stats.drop(columns=['team.id','team.name','team.logo'], inplace=True, axis=1)
df_away_stats = pd.json_normalize(df['away_statistics'])
df_away_stats.drop(columns=['team.id','team.name','team.logo'], inplace=True, axis=1)

df_home_stats.columns = ['home_' + col for col in df_home_stats.columns]
df_away_stats.columns = ['away_' + col for col in df_away_stats.columns]

df = df.drop(['home_statistics', 'away_statistics'], axis=1)
df = pd.concat([df, df_home_stats, df_away_stats], axis=1)

home_stats = df['home_statistics']
away_stats = df['away_statistics']

df = df.drop(['home_statistics', 'away_statistics'], axis=1)

df['home_statistics'] = home_stats.apply(process_statistics)
df['away_statistics'] = away_stats.apply(process_statistics)

home_stats = df['home_statistics']
away_stats = df['away_statistics']

df = df.drop(['home_statistics', 'away_statistics'], axis=1)

home_stats = pd.json_normalize(home_stats)
away_stats = pd.json_normalize(away_stats)

home_stats.columns = ['home_' + col for col in home_stats.columns]
away_stats.columns = ['away_' + col for col in away_stats.columns]

df = pd.concat([df, home_stats, away_stats], axis=1)
    
columns = df.columns
home_stats = df.filter(like='home')
away_stats = df.filter(like='away')

# Create new dataframes with normalized structure for home and away
df = df.drop(columns=home_stats.columns.tolist(), axis=1)
df = df.drop(columns=away_stats.columns.tolist(), axis=1)

df_home = pd.concat([df, home_stats], axis=1)
df_away = pd.concat([df, away_stats], axis=1)

# Add 'venue' and 'winner' columns
df_home['venue'] = 'home'
df_away['venue'] = 'away'

columns_df_home = df_home.columns.to_list()
columns_df_away = df_away.columns.to_list()

df_home.columns = [c.replace("home_", "").replace(".home", "") for c in columns_df_home]
df_away.columns = [c.replace("away_", "").replace(".away", "") for c in columns_df_away]

# Combine the dataframes
df = pd.concat([df_home, df_away], ignore_index=True)

today_str = today.strftime('%Y_%m_%d')
df.to_excel(f'fixtures_{today_str}.xlsx', index=False)