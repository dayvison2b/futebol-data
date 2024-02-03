import pandas as pd
import numpy as np

#Elo ratings calculations

"""
Existem dois modelos de ratings

1º
Rn = Ro + K * (W - We)

Where

Rn is the new rating
Ro is the old (pre-match) rating
K is the weight constant for the tournament played (values below)

K is then adjusted for the goal difference in the game.
It is increased by half if a game is won by two goals, by 3/4 if a game is won by three goals, and by 3/4 + (N-3)/8 if the game is won by four or more goals
where N is the goal difference.

W is the result of the game (1 for a win, 0.5 for a draw, and 0 for a loss).
We is the expected result (win expectancy), from the following formula:
We = 1 / (10(-dr/400) + 1)

where dr stands for the difference in ratings (we can add (75,80,100...) points for the home team)
-------------------------------------------------------------------------------------------

2º
Rn = Ro + K * G * (W - We)

Where 

G = number of goals
If the game is a draw or is won by one goal then G=1. If the game is won by two goals then G=1.5.
If the game is won by three or more goals the formula G= (11+N) / 8 is applied
"""
#Constants

# K = 60
WORLD_CUP_FINALS = []

# K = 50
CONTINENTAL_CHAMPIONSHIP_FINALS=[]
MAJOR_INTERCONTINENTAL_TOURNAMENTS=[]

# K = 40
WORLD_CUP = []
CONTINENTAL_QUALIFIERS=[]
MAJOR_TOURNAMENTS=[]

# K = 30
OTHER_TOURNAMENTS=['Serie A','Süper Lig','Premier League','La Liga','Eredivisie','Bundesliga']

# K = 20 FOR FRIENDLY MATCHES


def get_k(league):
    k=20
    if league in WORLD_CUP_FINALS:
        k=60
    elif league in CONTINENTAL_CHAMPIONSHIP_FINALS or league in MAJOR_INTERCONTINENTAL_TOURNAMENTS:
        k=50
    elif league in WORLD_CUP or league in CONTINENTAL_QUALIFIERS or league in MAJOR_TOURNAMENTS:
        k=40
    elif league in OTHER_TOURNAMENTS:
        k=30
    return k

def expected_result(home, away):
    elo_diff = (home + 100) - away
    we = 1 / (10 ** (-elo_diff / 400) +1)
    return [np.round(we, 3), 1 - np.round(we, 3)]

def actual_result(home_goals, away_goals):
    if home_goals > away_goals:
        home, away, g = 1, 0, home_goals - away_goals
    elif home_goals < away_goals:
        home, away, g = 0, 1, away_goals - home_goals
    else:
        home = away = 0.5
        g = 1 if home_goals == away_goals else None

    if g is not None and g != 1:
        g = 1.5 if g == 2 else (11 + g) / 8

    return [home, away, g]

def calculate_elo(elo_home,elo_away,home_goals,away_goals,league):
  k=get_k(league)
  home_result,away_result, g=actual_result(home_goals, away_goals)
  home_expected_result,away_expected_result=expected_result(elo_home,elo_away)
  
  elo_home=elo_home+k*g*(home_result-home_expected_result)
  elo_away=elo_away+k*g*(away_result-away_expected_result)
  
  return elo_home,elo_away, home_expected_result, away_expected_result