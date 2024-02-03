CREATE VIEW FIXTURES_STATS_BI AS
SELECT fixture.id
       ,fixture.date
       ,fixture.referee
       ,league.country AS 'league_country'
       ,league.name AS 'league_name'
       ,home_team.name AS 'team_name'
       ,home_statistic.score_extratime
       ,home_statistic.score_fulltime
       ,home_statistic.score_halftime
       ,home_statistic.score_penalty
       ,home_statistic.goals
       ,home_statistic.teams_winner
       ,home_statistic.`Shots on Goal`
       ,home_statistic.`Shots off Goal`
       ,home_statistic.`Total Shots`
       ,home_statistic.`Blocked Shots`
       ,home_statistic.`Shots insidebox`
       ,home_statistic.`Shots outsidebox`
       ,home_statistic.Fouls
       ,home_statistic.`Corner Kicks`
       ,home_statistic.Offsides
       ,home_statistic.`Ball Possession`
       ,home_statistic.`Yellow Cards`
       ,home_statistic.`Red Cards`
       ,home_statistic.`Goalkeeper Saves`
       ,home_statistic.`Total passes`
       ,home_statistic.`Passes accurate`
       ,home_statistic.`Passes %`
       ,home_statistic.expected_goals
       ,home_statistic.id AS 'statistic_id'
       ,home_statistic.venue
FROM FIXTURES AS fixture
LEFT JOIN LEAGUE AS league ON fixture.league_id = league.id
LEFT JOIN TEAM AS home_team ON fixture.home_id = home_team.id
LEFT JOIN STATISTIC AS home_statistic ON fixture.id = home_statistic.fixture_id
WHERE home_statistic.venue = 'home'

UNION ALL

SELECT fixture.id
       ,fixture.date
       ,fixture.referee
       ,league.country AS 'league_country'
       ,league.name AS 'league_name'
       ,away_team.name AS 'team_name'
       ,away_statistic.score_extratime
       ,away_statistic.score_fulltime
       ,away_statistic.score_halftime
       ,away_statistic.score_penalty
       ,away_statistic.goals
       ,away_statistic.teams_winner
       ,away_statistic.`Shots on Goal`
       ,away_statistic.`Shots off Goal`
       ,away_statistic.`Total Shots`
       ,away_statistic.`Blocked Shots`
       ,away_statistic.`Shots insidebox`
       ,away_statistic.`Shots outsidebox`
       ,away_statistic.Fouls
       ,away_statistic.`Corner Kicks`
       ,away_statistic.Offsides
       ,away_statistic.`Ball Possession`
       ,away_statistic.`Yellow Cards`
       ,away_statistic.`Red Cards`
       ,away_statistic.`Goalkeeper Saves`
       ,away_statistic.`Total passes`
       ,away_statistic.`Passes accurate`
       ,away_statistic.`Passes %`
       ,away_statistic.expected_goals
       ,away_statistic.id AS 'statistic_id'
       ,away_statistic.venue
FROM FIXTURES AS fixture
LEFT JOIN LEAGUE AS league ON fixture.league_id = league.id
LEFT JOIN TEAM AS away_team ON fixture.away_id = away_team.id
LEFT JOIN STATISTIC AS away_statistic ON fixture.id = away_statistic.fixture_id
WHERE away_statistic.venue = 'away';
