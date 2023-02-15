set jit = off;

-- ⚠ This variant implements K-Means using recursive UNION set semantics,
--   based on

--  ➊ a user-defined equality operator: = :: point × point → bool
--    (and aggregate AVG :: bag(point) → point)
\i points.sql

\i Users/admin/OneDrive/Desktop/points.sql


-- Set of points P that we will cluster
DROP TABLE IF EXISTS station_classification_19;
CREATE TABLE station_classification_19 (
  id            int GENERATED ALWAYS AS IDENTITY,
  start_station integer,                  -- unique point ID/label
  loc           point                     -- location of point in 2D space
);

--* We have considered this a one-dimensional clustering,
--  so the second dimension has been set as zero.
-- Instantiate P 
INSERT INTO station_classification_19(start_station, loc)
WITH 
 edit_t_19 AS(
 SELECT e.*
 FROM    edit_t_19_q1 AS e
    UNION ALL 
 SELECT e.*
 FROM    edit_t_19_q2 AS e
     UNION ALL 
 SELECT e.*
 FROM    edit_t_19_q3 AS e 
     UNION ALL 
 SELECT e.*
 FROM    edit_t_19_q4 AS e 
 )
 SELECT   e.start_station, point(COUNT(*), 0) 
 FROM     edit_t_19 AS e
 GROUP BY e.start_station;

--TABLE station_classification_19;

ALTER TABLE station_classification_19 ADD COLUMN class integer;
ALTER TABLE station_classification_19 ADD COLUMN mean point;

-- k_means QUERY

WITH RECURSIVE 
 -- k_means(‹i›, ‹p›, ‹c›, ‹mean›):
 --   in iteration ‹i›, point ID ‹p› has been assigned to cluster ID ‹c›,
 --   the centroid of ‹c› is point ‹mean›
 --   (i.e., there exists an FD ‹c› → ‹mean›).
 k_means (iter, id, cluster, mean)AS (
  SELECT 0  AS iter, s.id, ROW_NUMBER() OVER () AS cluster, s.loc AS mean
  --          🠵
  -- From P, choose a sample of points (these will become the
  -- initial cluster centers), assign unique cluster IDs
  FROM   station_classification_19 AS s
  WHERE  s.id IN (1 ,37 , 60, 189) -- choose points as initial cluster centers
    UNION ALL
      -- 2. Update
  SELECT (assign.iter + 1) AS iter, assign.id, assign.cluster,
  --      🠷                   
         --AVG(assign.loc) OVER cluster AS mean
       AVG(assign.loc) OVER cluster AS mean
  FROM   (SELECT DISTINCT ON (s.id) k.iter, s.id, k.cluster, s.loc
          FROM    station_classification_19 AS s, k_means AS k
          ORDER BY s.id, s.loc <-> k.mean
               ) AS assign(iter, id, cluster, loc)
  WHERE assign.iter < 15 
  WINDOW cluster AS (PARTITION BY assign.cluster)
 ), 
 --Check whether the selected iter led to the same result in the two last iterations.
 check_iter AS(
  SELECT k.id, k.cluster, k.mean
  FROM   k_means AS k
  WHERE  k.iter :: int =  (SELECT MAX(k1.iter)
                           FROM   k_means AS k1)
           EXCEPT 

  SELECT k.id, k.cluster, k.mean
  FROM   k_means AS k
  WHERE  k.iter :: int = (SELECT MAX(k1.iter)-1
                           FROM   k_means AS k1)                  
 ),
 -- the output of the last iteration has been considered as the result of the query
 k_means_result AS(
  SELECT k.id, DENSE_RANK () OVER(ORDER BY k.mean DESC) AS cluster, k.mean
  FROM   k_means AS k
  WHERE  k.iter :: int =  (SELECT MAX(k1.iter)
                           FROM   k_means AS k1)               
 ),
 --total within-cluster sum of square (WCSS)
 WCSS AS(
  SELECT SUM ((k.mean[0] - s.loc[0])^ 2 + (k.mean[1] - s.loc[1])^2) AS WCSS 
  FROM   k_means_result AS k, station_classification_19 AS s
  WHERE  k.id = s.id
 )
 UPDATE station_classification_19 AS s SET
  class = k.cluster,
  mean = k.mean
 FROM   k_means_result AS k
 WHERE  k.id = s.id;

 --TABLE check_iter;
 --TABLE WCSS;
 --TABLE k_means_result;


-- k: 2               WCSS = 321,682,762
-- k: 3               WCSS = 112,967,183
-- k: 4               WCSS =  67,167,748    -->> selected
-- k: 5               WCSS =  41,147,626

