import database
import pandas as pd

select_matches = database.select_documents('matches')
select_shootings = database.select_documents('shooting')

matches = pd.DataFrame(select_matches)
shootings = pd.DataFrame(select_shootings)

# Convert 'Date' columns to datetime objects
matches['Date'] = pd.to_datetime(matches['Date'])
shootings['date'] = pd.to_datetime(shootings['date'])

shootings = shootings[~shootings.duplicated()]

# Merge based on 'team' and 'Date'
merged_df = pd.merge(matches, shootings, left_on=['Team', 'Date'], right_on=['team', 'date'], how='inner')

# Drop duplicate columns, if any
merged_df.drop(columns=['team', 'date'], inplace=True)

merged_df.columns = [c.lower() for c in merged_df.columns]
merged_df['notes'] = None
merged_df.rename(columns={'goals for': 'gf', 'goals against': 'ga'}, inplace=True)

merged_df.to_csv('matches.csv', index=False)