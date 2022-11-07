#Task 1 Data Ingestion 
#Load the tables created with JavaScript Object Notation (JSON) and CSV data format into the dataset using the following information:

Field	Value
Source	Cloud Storage
Select file from Cloud Storage bucket	spls/bq-soccer-analytics/events.json
File format	JSONL (Newline delimited JSON)
Table name	Table name
Schema	Check the box marked Schema Auto detect

#Loaded another table of soccer data format CSV into the dataset using the following information below:

Field	Value
Source	Cloud Storage
Select file from Cloud Storage bucket	spls/bq-soccer-analytics/tags2name.csv
File format	CSV
Table name	Table name
Schema	Check the box marked Auto detect

#Task 2
#Built a query that shows the success rate on penalty kicks by each player.
#Join the Table name table with the players table to get player names from their IDs
#Filter on penalty kicks
#Group by player ID and player name
#Player should attempt at least 5 penalty kicks
#Order by penalty kick success rate

SELECT
playerId,
(Players.firstName || ' ' || Players.lastName) AS playerName,
COUNT(id) AS numPKAtt,
SUM(IF(101 IN UNNEST(tags.id), 1, 0)) AS numPKGoals,
SAFE_DIVIDE(
SUM(IF(101 IN UNNEST(tags.id), 1, 0)),
COUNT(id)
) AS PKSuccessRate
FROM
`soccer.EVENT_NAME` Events
LEFT JOIN
`soccer.players` Players ON
Events.playerId = Players.wyId
WHERE
eventName = 'Free Kick' AND
subEventName = 'Penalty'
GROUP BY
playerId, playerName
HAVING
numPkAtt >= 5
ORDER BY
PKSuccessRate DESC, numPKAtt DESC

#Task 3
#Created a new query to analyze shot distance. For shots, use (x, y) values from the positions field in the Table name table.
#Have to
#Calculate shot distance using the midpoint of the goal mouth ( X-axis goal mouth length , Y-axis goal mouth length ) as the ending location.
#Calculate pass distance by x-coordinate and y-coordinate differences, 
#then convert to estimated meters using the average dimensions of a soccer field ( X-axis length x Y-axis length ).
#Add an isGoal field by looking "inside" the tags field.
#Filter the Table name table to shots only.
#Shot distance must be less than 50.
#The final SELECT statement aggregates the number of shots, 
#the number of goals and the percentage of goals from shots by distance rounded to the nearest meter.


WITH
Shots AS
(
SELECT
*,
/* 101 is known Tag for 'goals' from goals table */
(101 IN UNNEST(tags.id)) AS isGoal,
/* Translate 0-100 (x,y) coordinate-based distances to absolute positions
using "average" field dimensions of 105x68 before combining in 2D dist calc */
SQRT(
POW(
  (100 - positions[ORDINAL(1)].x) * 120/100,
  2) +
POW(
  (60 - positions[ORDINAL(1)].y) * 69/100,
  2)
 ) AS shotDistance
FROM
`soccer.EVENT_NAME`
WHERE
/* Includes both "open play" & free kick shots (including penalties) */
eventName = 'Shot' OR
(eventName = 'Free Kick' AND subEventName IN ('Free kick shot', 'Penalty'))
)
SELECT
ROUND(shotDistance, 0) AS ShotDistRound0,
COUNT(*) AS numShots,
SUM(IF(isGoal, 1, 0)) AS numGoals,
AVG(IF(isGoal, 1, 0)) AS goalPct
FROM
Shots
WHERE
shotDistance <= 50
GROUP BY
ShotDistRound0
ORDER BY
ShotDistRound0

#Task 4 
#Create a regression model using soccer data
#Creating some user-defined functions in BigQuery that help with shot distance and angle calculations, 
#which help to prepare the soccer event data for eventual use in an ML model.

#Calculate shot distance from (x,y) coordinates
#Defining a function shot distance to goal for 
#calculating the shot distance from (x,y) coordinates in the soccer dataset using the following code-blocks.
CREATE FUNCTION `shot distance to goal`(x INT64, y INT64)
RETURNS FLOAT64
AS (
 /* Translate 0-100 (x,y) coordinate-based distances to absolute positions
 using "average" field dimensions of X-axis lengthxY-axis length before combining in 2D dist calc */
 SQRT(
   POW((X-axis goal mouth length - x) * X-axis length/100, 2) +
   POW((Y-axis goal mouth length - y) * Y-axis length/100, 2)
   )
 );

#Calculate shot angle from (x,y) coordinates
#Define a function shot angle to goal 
#for calculating the shot angle from (x,y) coordinates in the soccer dataset using the following code-blocks.
CREATE FUNCTION `shot angle to goal`(x INT64, y INT64)
RETURNS FLOAT64
AS (
 SAFE.ACOS(
   /* Have to translate 0-100 (x,y) coordinates to absolute positions using
   "average" field dimensions of X-axis lengthxY-axis length before using in various distance calcs */
   SAFE_DIVIDE(
     ( /* Squared distance between shot and 1 post, in meters */
       (POW(X-axis length - (x * X-axis length/100), 2) + POW(Y-axis half + (7.32/2) - (y * Y-axis length/100), 2)) +
       /* Squared distance between shot and other post, in meters */
       (POW(X-axis length - (x * X-axis length/100), 2) + POW(Y-axis half - (7.32/2) - (y * Y-axis length/100), 2)) -
       /* Squared length of goal opening, in meters */
       POW(7.32, 2)
     ),
     (2 *
       /* Distance between shot and 1 post, in meters */
       SQRT(POW(X-axis length - (x * X-axis length/100), 2) + POW(Y-axis half + 7.32/2 - (y * Y-axis length/100), 2)) *
       /* Distance between shot and other post, in meters */
       SQRT(POW(X-axis length - (x * X-axis length/100), 2) + POW(Y-axis half - 7.32/2 - (y * Y-axis length/100), 2))
     )
    )
  /* Translate radians to degrees */
  ) * 180 / ACOS(-1)
 )
;
#Task 5
#Creating expected goals model using BigQuery ML
#Building an expected goals model from the soccer event data to predict 
#the likelihood of a shot going in for a goal given its type, distance, and angle.


SELECT
predicted_isGoal_probs[ORDINAL(1)].prob AS predictedGoalProb,
* EXCEPT (predicted_isGoal, predicted_isGoal_probs),
FROM
ML.PREDICT(
MODEL `soccer.xg_logistic_reg_model_238`, 
(
 SELECT
   Events.playerId,
   (Players.firstName || ' ' || Players.lastName) AS playerName,
   Teams.name AS teamName,
   CAST(Matches.dateutc AS DATE) AS matchDate,
   Matches.label AS match,
 /* Convert match period and event seconds to minute of match */
   CAST((CASE
     WHEN Events.matchPeriod = '1H' THEN 0
     WHEN Events.matchPeriod = '2H' THEN 45
     WHEN Events.matchPeriod = 'E1' THEN 90
     WHEN Events.matchPeriod = 'E2' THEN 105
     ELSE 120
     END) +
     CEILING(Events.eventSec / 60) AS INT64)
     AS matchMinute,
   Events.subEventName AS shotType,
   /* 101 is known Tag for 'goals' from goals table */
   (101 IN UNNEST(Events.tags.id)) AS isGoal,
 
   `soccer.GetShotDistanceToGoal238`(Events.positions[ORDINAL(1)].x,
       Events.positions[ORDINAL(1)].y) AS shotDistance,
   `soccer.GetShotAngleToGoal238`(Events.positions[ORDINAL(1)].x,
       Events.positions[ORDINAL(1)].y) AS shotAngle
 FROM
   `soccer.events238` Events
 LEFT JOIN
   `soccer.matches` Matches ON
       Events.matchId = Matches.wyId
 LEFT JOIN
   `soccer.competitions` Competitions ON
       Matches.competitionId = Competitions.wyId
 LEFT JOIN
   `soccer.players` Players ON
       Events.playerId = Players.wyId
 LEFT JOIN
   `soccer.teams` Teams ON
       Events.teamId = Teams.wyId
 WHERE
   /* Look only at World Cup matches to apply model */
   Competitions.name = 'World Cup' AND
   /* Includes both "open play" & free kick shots (but not penalties) */
   (
     eventName = 'Shot' OR
     (eventName = 'Free Kick' AND subEventName IN ('Free kick shot'))
   ) AND
   /* Filter only to goals scored */
   (101 IN UNNEST(Events.tags.id))
)
)
ORDER BY
predictedgoalProb

