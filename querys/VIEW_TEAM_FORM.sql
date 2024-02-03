CREATE VIEW VIEW_TEAM_FORM AS 
SELECT 
    fixtures.`team_name` AS team_name,
    LEFT(fixtures.date, 4) AS season,
    (SUM(CASE WHEN fixtures.`teams_winner` = 1 THEN 1 ELSE 0 END)) AS V,
    (SUM(CASE WHEN fixtures.`teams_winner` IS NULL THEN 1 ELSE 0 END)) AS E,
    COUNT(*) AS PJ,
    ((SUM(CASE WHEN fixtures.`teams_winner` = 1 THEN 1 ELSE 0 END) * 3) + (SUM(CASE WHEN fixtures.`teams_winner` IS NULL THEN 1 ELSE 0 END) * 1)) / COUNT(*) AS 'FORM'
FROM 
    FIXTURES_STATS_BI AS fixtures
GROUP BY 
    fixtures.`team_name`,
    LEFT(fixtures.date, 4);
    
SELECT * FROM FIXTURES_STATS_BI WHERE team_name like '%Vasco%'