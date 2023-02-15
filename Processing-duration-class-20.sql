set jit = off;

-- ⚠ This variant implements K-Means using recursive UNION set semantics,
--   based on

--  ➊ a user-defined equality operator: = :: point × point → bool
--    (and aggregate AVG :: bag(point) → point)
\i points.sql

\i Users/admin/OneDrive/Desktop/points.sql


-- Set of points P that we will cluster
DROP TABLE IF EXISTS duration_classification_20;
CREATE TABLE duration_classification_20 (
  id            int GENERATED ALWAYS AS IDENTITY,
  duration      interval,                 -- unique point ID/label
  loc           point                     -- location of point in 2D space
);

-- Instantiate P 
INSERT INTO duration_classification_20 (duration, loc)
WITH
 edit_t_20 AS(
 SELECT e.*
 FROM    edit_t_20_q1 AS e
    UNION ALL 
 SELECT e.*
 FROM    edit_t_20_q2 AS e
     UNION ALL 
 SELECT e.*
 FROM    edit_t_20_q3 AS e 
     UNION ALL 
 SELECT e.*
 FROM    edit_t_20_q4 AS e 
 )
 SELECT DISTINCT ON(e.duration) e.duration,
          point((EXTRACT(EPOCH FROM e.duration)/60) , COUNT(*)) 
 FROM     edit_t_20 AS e
 GrOUP BY e.duration;

--TABLE duration_classification_20
--ORDER BY class;

ALTER TABLE duration_classification_20 ADD COLUMN class integer;
ALTER TABLE duration_classification_20 ADD COLUMN mean point;

-- k_means QUERY

WITH RECURSIVE
 -- k_means(‹i›, ‹p›, ‹c›, ‹mean›):
 --   in iteration ‹i›, point ID ‹p› has been assigned to cluster ID ‹c›,
 --   the centroid of ‹c› is point ‹mean›
 --   (i.e., there exists an FD ‹c› → ‹mean›).
 k_means (iter, id, cluster, mean)AS (
  SELECT 0  AS iter, d.id, ROW_NUMBER() OVER () AS cluster, d.loc AS mean
  --          🠵
  -- From P, choose a sample of points (these will become the
  -- initial cluster centers), assign unique cluster IDs
  FROM   duration_classification_20 AS d
  WHERE  d.id IN (4 ,93, 245, 1043 ) -- choose points as initial cluster centers
    UNION ALL
      -- 2. Update
  SELECT (assign.iter + 1) AS iter, assign.id, assign.cluster,
  --      🠷                   
         --AVG(assign.loc) OVER cluster AS mean
       AVG(assign.loc) OVER cluster AS mean
  FROM   (SELECT DISTINCT ON (d.id) k.iter, d.id, k.cluster, d.loc
          FROM    duration_classification_20 AS d, k_means AS k
          ORDER BY d.id, d.loc <-> k.mean
               ) AS assign(iter, id, cluster, loc)
  WHERE assign.iter < 18
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
  SELECT SUM ((k.mean[0] - d.loc[0])^ 2 + (k.mean[1] - d.loc[1])^2) AS WCSS 
  FROM   k_means_result AS k, duration_classification_20 AS d
  WHERE  k.id = d.id
 )
 UPDATE duration_classification_20 AS d SET
  class = k.cluster,
  mean  = k.mean
 FROM   k_means_result AS k
 WHERE  k.id = d.id;
 --TABLE check_iter;
 --TABLE WCSS;
 --TABLE k_means_result;




-- k: 2    ->         WCSS = 688,669,412
-- k: 3    ->         WCSS = 540,189,001
-- k: 4    ->         WCSS = 212,774,089    -->> selected
-- k: 5    ->         WCSS = 186,085,910


