from datetime import datetime
import pandas as pd
import joblib
import json
from database import select_documents_by_where

def make_prediction(data):
    # Carregar os dados históricos
    matches_rolling = pd.read_csv("matches.csv")
    new_match_data = pd.DataFrame(data, index=[0])
    new_match_data.columns = [c.lower() for c in new_match_data.columns]
    team = new_match_data["team"].iloc[0]

    if pd.isnull(new_match_data["formation"]).all():
        try:
            most_common_formation = matches_rolling.loc[matches_rolling["team"] == team, "formation"].mode().iloc[0]
        except:
            print('Team not found in matches.csv')
            return
        # Preencher 'formation' com o valor mais frequente
        new_match_data["formation"] = most_common_formation
        
    # Concatenar novas partidas com o conjunto de dados histórico
    combined_data = pd.concat([matches_rolling, new_match_data], ignore_index=True)
    combined_data = treatment(combined_data)

    cols = ["gf", "ga", "sh", "sot", "dist", "fk", "pk", "pkatt", "poss","g/sot","g/sh"]
    new_cols = [f"{c}_rolling" for c in cols]
    selected_features = ["venue_code", "opp_code", "day_code", "formation_code","comp_code"] + new_cols

    # Recalcular as médias móveis para o conjunto combinado
    combined_data = combined_data.groupby("team").apply(lambda x: rolling_averages(x, cols, new_cols))
    combined_data = combined_data.reset_index(drop=True)

    # Preencher valores ausentes nas médias móveis com a média global
    #combined_data.fillna(matches_rolling[new_cols].mean(), inplace=True)
    #matches = matches.dropna()

    # Selecionar as features relevantes para as novas partidas
    new_match_date_value = new_match_data["date"].iloc[0]
    new_match_features = combined_data[pd.to_datetime(combined_data['date']) >= pd.to_datetime(new_match_date_value)][selected_features]
    

    # Fazer a predição usando o modelo treinado
    prediction = best_rf.predict(new_match_features)
    probability = best_rf.predict_proba(new_match_features)[:, 1]  # Probabilidade de vitória

    # Criar um dicionário com os resultados
    prediction_result = {
        "team": str(new_match_data["team"].iloc[0]),
        "date": str(new_match_data["date"].iloc[0].strftime('%Y-%m-%d')),
        "opponent": str(new_match_data["opponent"].iloc[0]),
        "venue": str(new_match_data['venue'].iloc[0]),
        "comp": str(new_match_data["comp"].iloc[0]),
        "formation": str(new_match_data["formation"].iloc[0]),
        "prediction": str(prediction[0]),
        "probability": str(probability[0])
    }

    return prediction_result

def rolling_averages(group, cols, new_cols):
    group = group.sort_values("date")
    rolling_stats = group[cols].rolling(3, closed='left').mean()
    group[new_cols] = rolling_stats
    group = group.dropna(subset=new_cols)
    return group

def treatment(dataframe):
    dataframe_bkp = dataframe.tail(1).copy()  # Create a copy of the last row
    dataframe = dataframe[~dataframe.isin(dataframe_bkp)]
    dataframe.drop(columns=['notes', 'attendance', 'captain'], axis=1, inplace=True)
    dataframe_bkp.drop(columns=['notes', 'attendance', 'captain'], axis=1, inplace=True)
    dataframe = dataframe[~dataframe['result'].isna()]
    dataframe = dataframe.dropna()
    dataframe = pd.concat([dataframe, dataframe_bkp], ignore_index=True)
    dataframe["date"] = pd.to_datetime(dataframe["date"])
    dataframe['formation'] = dataframe['formation'].str.replace('◆', '')
    dataframe['gf'] = dataframe['gf'].apply(lambda x: int(str(x).split('(')[0].strip()) if '(' in str(x) else x)
    dataframe['ga'] = dataframe['ga'].apply(lambda x: int(str(x).split('(')[0].strip()) if '(' in str(x) else x)
    dataframe["venue_code"] = dataframe["venue"].astype("category").cat.codes
    dataframe["opp_code"] = dataframe["opponent"].astype("category").cat.codes
    dataframe["formation_code"] = dataframe["formation"].astype("category").cat.codes
    dataframe['comp_code'] = dataframe["comp"].astype("category").cat.codes
    # dataframe["hour"] = dataframe["time"].str.replace(":.+", "", regex=True).astype("int")
    dataframe["day_code"] = dataframe["date"].dt.dayofweek
    return dataframe


today = datetime.today()
current_time = today.time().strftime("%H:%M")
today = today.strftime("%Y-%m-%d")

matches = select_documents_by_where('matches', ('Date', '>=', today))

# Filter matches based on the current time
filtered_matches = [
    match for match in matches
    if (
        str(match['Date']) == today and
        not pd.isna(match['Time']) and
        str(match['Time']) >= current_time
    ) or (
        str(match['Date']) > today  # For future dates, include all times
    )
]
matches = pd.DataFrame(filtered_matches)
matches.rename(columns={'Goals against': 'ga', 'Goals for': 'gf'}, inplace=True)
matches['Date'] = pd.to_datetime(matches['Date'])
matches.sort_values(by='Date',ascending=True, inplace=True)
matches = matches[matches['Date'] >= today]
allowed_matches = []
for match in matches.itertuples(index=False):
    if not allowed_matches:
        allowed_matches.append(match)
        continue
    is_team_allowed = all(match.Team not in allowed_match.Opponent for allowed_match in allowed_matches)
    if is_team_allowed:
        allowed_matches.append(match)
matches = pd.DataFrame(allowed_matches).head(100)
matches.to_csv('new_100_matches.csv', index=False)

best_rf = joblib.load("random_forest_model.joblib")

future_matches = {}

for _, match in matches.iterrows():
    new_match_data = match.to_dict()
    prediction_result = make_prediction(new_match_data)
    
    if prediction_result is not None:
        team_name = new_match_data["Team"]
        if team_name in future_matches:
            future_matches[team_name].append(prediction_result)
        else:
            future_matches[team_name] = [prediction_result]
    else:
        raise ValueError(f"The prediction result failed for match {new_match_data}")

json_file = 'future_matches.json'
with open(json_file, 'w', encoding='utf-8') as json_file:
    json.dump(future_matches, json_file, ensure_ascii=False, indent=2)