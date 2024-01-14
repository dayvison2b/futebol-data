import pandas as pd

pd.set_option('display.max_columns', None)

file_path = 'files/fixtures_to_predict.json'
fixtures = pd.read_json(file_path, encoding='ISO-8859-1')

# Normalize columns with nested JSON structures
fixtures_normalized = pd.json_normalize(fixtures.to_dict(orient='records'))

fixtures_normalized = fixtures_normalized[fixtures_normalized['home_statistics'].notna() & (fixtures_normalized['home_statistics'].astype(bool))]
fixtures_normalized.reset_index(drop=True, inplace=True)

# Normalize 'home_statistics' and 'away_statistics' columns
home_stats_normalized = pd.json_normalize(fixtures_normalized['home_statistics'].apply(lambda x: x[0] if x else {}))
away_stats_normalized = pd.json_normalize(fixtures_normalized['away_statistics'].apply(lambda x: x[0] if x else {}))
home_stats_normalized['original_order'] = home_stats_normalized.index
away_stats_normalized['original_order'] = away_stats_normalized.index

# Normalize 'statistics' column in both DataFrames
home_stats_statistics_normalized = pd.json_normalize(home_stats_normalized.to_dict('records'), record_path='statistics', meta=['original_order', 'team.id'])
away_stats_statistics_normalized = pd.json_normalize(away_stats_normalized.to_dict('records'), record_path='statistics', meta=['original_order', 'team.id'])

home_stats_statistics_normalized = home_stats_statistics_normalized.pivot(index=['original_order', 'team.id'], columns='type', values='value').add_prefix('home_stats_')
away_stats_statistics_normalized = away_stats_statistics_normalized.pivot(index=['original_order', 'team.id'], columns='type', values='value').add_prefix('away_stats_')

fixtures_normalized = fixtures_normalized.merge(home_stats_statistics_normalized, how='outer', left_index=True, right_on='original_order').reset_index(drop=True)
fixtures_normalized = fixtures_normalized.merge(away_stats_statistics_normalized, how='outer', left_index=True, right_on='original_order').reset_index(drop=True)

# Drop irrelevant or repeated columns
columns_to_drop = [
    'score.extratime.away', 'score.extratime.home', 'score.fulltime.away', 'score.fulltime.home',
    'score.halftime.away', 'score.halftime.home', 'score.penalty.away', 'score.penalty.home',
    'teams.away.logo', 'teams.away.name', 'teams.home.logo', 'teams.home.name',
    'fixture.id', 'fixture.timezone', 'fixture.status.long', 'fixture.status.short',
    'fixture.status.elapsed', 'fixture.venue.id', 'fixture.venue.city', 'fixture.venue.name',
    'fixture.periods.first', 'fixture.periods.second', 'fixture.timestamp', 'league.flag',
    'league.season', 'league.id', 'league.round', 'league.country', 'league.logo',
    'league.name', 'home_statistics', 'away_statistics'
]
fixtures_cleaned = fixtures_normalized.drop(columns=columns_to_drop)


# Rename columns for clarity
fixtures_cleaned.rename(columns={
    'goals.away': 'away_goals',
    'goals.home': 'home_goals',
    'teams.away.winner': 'away_winner',
    'teams.away.id': 'away_id',
    'teams.home.winner': 'home_winner',
    'teams.home.id': 'home_id',
    'fixture.date': 'fixture_date',
    'fixture.referee': 'referee',
    'home_stats_Ball Possession': 'home_ball_possession',
    'home_stats_Blocked Shots': 'home_blocked_shots',
    'home_stats_Corner Kicks': 'home_corner_kicks',
    'home_stats_Fouls': 'home_fouls',
    'home_stats_Goalkeeper Saves': 'home_goalkeeper_saves',
    'home_stats_Offsides': 'home_offsides',
    'home_stats_Passes %': 'home_passes_percentage',
    'home_stats_Passes accurate': 'home_passes_accurate',
    'home_stats_Red Cards': 'home_red_cards',
    'home_stats_Shots insidebox': 'home_shots_inside_box',
    'home_stats_Shots off Goal': 'home_shots_off_goal',
    'home_stats_Shots on Goal': 'home_shots_on_goal',
    'home_stats_Shots outsidebox': 'home_shots_outside_box',
    'home_stats_Total Shots': 'home_total_shots',
    'home_stats_Total passes': 'home_total_passes',
    'home_stats_Yellow Cards': 'home_yellow_cards',
    'home_stats_expected_goals': 'home_expected_goals',
    'away_stats_Ball Possession': 'away_ball_possession',
    'away_stats_Blocked Shots': 'away_blocked_shots',
    'away_stats_Corner Kicks': 'away_corner_kicks',
    'away_stats_Fouls': 'away_fouls',
    'away_stats_Goalkeeper Saves': 'away_goalkeeper_saves',
    'away_stats_Offsides': 'away_offsides',
    'away_stats_Passes %': 'away_passes_percentage',
    'away_stats_Passes accurate': 'away_passes_accurate',
    'away_stats_Red Cards': 'away_red_cards',
    'away_stats_Shots insidebox': 'away_shots_inside_box',
    'away_stats_Shots off Goal': 'away_shots_off_goal',
    'away_stats_Shots on Goal': 'away_shots_on_goal',
    'away_stats_Shots outsidebox': 'away_shots_outside_box',
    'away_stats_Total Shots': 'away_total_shots',
    'away_stats_Total passes': 'away_total_passes',
    'away_stats_Yellow Cards': 'away_yellow_cards',
    'away_stats_expected_goals': 'away_expected_goals'
}, inplace=True)