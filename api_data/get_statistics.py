from datetime import datetime, timedelta
import asyncio
import httpx
import pandas as pd

from sqlalchemy import create_engine

from path_handler import add_parent_directory_to_path
add_parent_directory_to_path()

from api_data_request import make_request, base_url, headers, make_request_async
from utils.firestore_handler import select_documents

# Constants
MYSQL_HOST = '35.199.102.97'
MYSQL_PORT = '3306'
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = ''
MYSQL_DB_NAME = 'Futebol-data'

REQUESTS_PER_MINUTE = 300
MINUTE = 60

# Database connection setup
ENGINE = create_engine(f"mysql+mysqlconnector://{MYSQL_USERNAME}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}")

def create_table_from_columns(df, column_keywords):
    columns_to_filter = [column for column in df.columns if any(c in column for c in column_keywords)]
    return df.loc[:, columns_to_filter].drop_duplicates()

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
        conditions=[("fixture.date", "<=", date_init)
        ]
        ,limit=100
    )
    return fixtures

today = datetime.today() - timedelta(days=1)
date_init = today.strftime("%Y-%m-%d")
fixtures = get_fixtures(date_init)
fixtures_sorted = sorted(fixtures, key=lambda x: datetime.fromisoformat(x['fixture']['date']), reverse=True)

# Variáveis de controle de taxa
import time
request_count = 0
start_time = time.time()

async def fetch_statistics_async(session, fixture, base_url, headers):
    global request_count, start_time
    
    # Verifica se excedeu o limite de requisições por minuto
    elapsed_time = time.time() - start_time
    if elapsed_time > MINUTE:
        request_count = 0
        start_time = time.time()
        
    # Aguarda até que o limite de requisições permitidas por minuto seja respeitado
    while request_count >= REQUESTS_PER_MINUTE:
        await asyncio.sleep(1)
        
        
    home_team_id = fixture['teams']['home']['id']

    result = await make_request_async(session, base_url, 'fixtures/statistics', f'?fixture={fixture["fixture"]["id"]}', headers)
    result_copy = result.copy()
    
    try:
        if result[0]['team']['id'] == home_team_id:
            result_copy.pop(0)
            result.pop()

            result_away = result_copy
            result_home = result
    except Exception as e:
        raise Exception((result, fixture['fixture']['id']), e)
    # Atualiza o contador de requisições
    request_count += 1
        
    return result_home, result_away

async def main():
    async with httpx.AsyncClient(timeout=30) as session:  # Usando aiohttp.ClientSession como um contexto assíncrono
        for fixture in fixtures_sorted:
            statistics_results = await fetch_statistics_async(session, fixture, base_url, headers)

            # Update the fixture dictionary with the statistics
            fixture.update({
                'home_statistics': statistics_results[0],
                'away_statistics': statistics_results[1]
            })
            
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
        
    home_stats = df.filter(regex='home|fixture\\.id')
    away_stats = df.filter(regex='away|fixture\\.id')

    #Creating some ids
    home_stats['statistic_id'] = home_stats['fixture.id'].astype(str) + "_" + df['teams.home.id'].astype(str)
    away_stats['statistic_id'] = away_stats['fixture.id'].astype(str) + "_" + df['teams.away.id'].astype(str)
    df['teams_ids'] = home_stats['teams.home.id'].astype(str) + "_" + away_stats['teams.away.id'].astype(str)
    
    home_stats.drop(columns='fixture.id', axis=1, inplace=True)
    away_stats.drop(columns='fixture.id', axis=1, inplace=True)
    
    df['fixture.winner'] = df.apply(lambda x: 'home' if x['teams.home.winner'] else ('away' if x['teams.away.winner'] else 'draw'), axis=1)

    # Create new dataframes with normalized structure for home and away
    df = df.drop(columns=home_stats.columns.tolist(), axis=1, errors='ignore')
    df = df.drop(columns=away_stats.columns.tolist(), axis=1, errors='ignore')

    df_home = pd.concat([df, home_stats], axis=1)
    df_away = pd.concat([df, away_stats], axis=1)

    # Add 'venue' and 'winner' columns
    df_home['venue'] = 'home'
    df_away['venue'] = 'away'

    columns_df_home = df_home.columns.to_list()
    columns_df_away = df_away.columns.to_list()

    df_home.columns = [c.replace("home_", "").replace(".home", "") for c in columns_df_home]
    df_away.columns = [c.replace("away_", "").replace(".away", "") for c in columns_df_away]
    
    df_home.reset_index(drop=True, inplace=True)
    df_away.reset_index(drop=True, inplace=True)

    # Combine the dataframes
    df = pd.concat([df_home, df_away], ignore_index=True)
    
    #Catching teams ids
    teams_ids = df['teams_ids'].str.split('_', expand=True)
    df['home_id'], df['away_id'] = teams_ids[0], teams_ids[1]
    df.drop(columns='teams_ids', axis=1, inplace=True)
    
    fixture_table = create_table_from_columns(df, ['fixture', 'home_id', 'away_id', 'league.id','league.round']).reset_index(drop=True).drop_duplicates()
    
    league_table = create_table_from_columns(df, ['league'])
    league_table.drop(columns='league.round', inplace=True)
    league_table = league_table.reset_index(drop=True).drop_duplicates()
    
    
    team_table = create_table_from_columns(df, ['team'])
    team_table.drop(columns='teams.winner', inplace=True)
    team_table = team_table.reset_index(drop=True)
    team_table = team_table.drop_duplicates()
    statistics_table = df['fixture.id']
    
    columns_to_drop = list(fixture_table.columns) + list(league_table.columns) + list(team_table.columns)
    df.drop(columns=columns_to_drop, inplace=True)

    statistics_table = pd.concat([statistics_table, df], axis=1).reset_index(drop=True).drop_duplicates()
    
    #Final treatments
    fixture_table.columns = [c.replace('fixture.', '').replace('.','_') for c in fixture_table.columns]
    league_table.columns = [c.replace('league.', '').replace('.','_') for c in league_table.columns]
    team_table.columns = [c.replace('teams.', '').replace('.','_') for c in team_table.columns]
    statistics_table.columns = [c.replace('statistic_', '').replace('.','_') for c in statistics_table.columns]
    
    
    tables = {
        'FIXTURES': fixture_table,
        'LEAGUE': league_table,
        'TEAM': team_table,
        'STATISTIC': statistics_table
    }

    # Export DataFrames to MySQL
    for table_name, table_data in tables.items():
        table_data.to_sql(name=table_name, con=ENGINE, index=False, if_exists='append')

    print(f'data exported to MySQL successfully.')
    
if __name__ == "__main__":
    asyncio.run(main())