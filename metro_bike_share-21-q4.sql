
DROP TABLE IF EXISTS edit_t_21_q4 CASCADE;
CREATE TABLE edit_t_21_q4 OF mbs_trips; 

INSERT INTO edit_t_21_q4
WITH

 check_ids(trip_id) AS(
    SELECT check_id(t.trip_id)
    FROM   trips_2021_q4 AS t
),
  check_durations(trip_id, duration) AS(
    SELECT t.trip_id::int, 
           check_duration(t.duration) AS duration
    FROM   trips_2021_q4 AS t
),
  check_dates (trip_id, start_time, end_time) AS (
    SELECT t.trip_id::int,
           Check_date(t.start_time) AS start_time,
           Check_date(t.end_time  ) AS end_time
    FROM   trips_2021_q4 AS t
), 
 check_stations(trip_id, start_station, end_station) AS(
    SELECT t.trip_id::int,
           check_station(t.start_station) AS start_station,
           check_station(t.end_station  ) AS end_time
    FROM   trips_2021_q4 AS t
),
 check_bike_ids(trip_id, bike_id) AS(
    SELECT t.trip_id::int,
           check_b_ids(t.trip_id, t.bike_id) AS bike_id
    FROM   trips_2021_q4 AS t
),
check_lat_lons(trip_id, start_lat, start_lon, end_lat, end_lon) AS(
     SELECT t.trip_id::int,
            check_lat_lon(t.start_lat) AS start_lat, 
            check_lat_lon(t.start_lon) AS start_lon,
            check_lat_lon(t.end_lat  ) AS end_lat, 
            check_lat_lon(t.end_lon  ) AS end_lon            
     FROM   trips_2021_q4 AS t    
),
check_lat_lons_processed (trip_id, start_lat, start_lon, end_lat, end_lon) AS(
      SELECT *
      FROM   check_lat_lons AS c
           EXCEPT 
      SELECT *                     -- Eleminate Data for :  
      FROM   check_lat_lons AS c   --start_station = 3000  Virtual station
      WHERE c.start_lat = 0        --start_station = 4286 Out of Service area Smart bike
         OR c.start_lon = 0        --start_station = 4285  Free bike
         OR c.end_lat = 0 
         OR c.end_lon = 0 
),
check_plan_durations(trip_id, plan_duration) AS(
      SELECT t.trip_id:: int,
             check_plan_duration(t.plan_duration) AS plan_duration 
      FROM   trips_2021_q4 AS t
              EXCEPT 
      SELECT t.trip_id:: int,
             check_plan_duration(t.plan_duration) AS plan_duration 
      FROM   trips_2021_q4 AS t
      WHERE  t.plan_duration = '999'  -- plan_duration = '999' is for testing
                                      -- smart bike at Virtual station
),
check_texts(trip_id, trip_route_category, passholder_type, bike_type) AS(
      SELECT t.trip_id::int,
             check_text(t.trip_route_category) AS trip_route_category,
             check_text(t.passholder_type)     AS passholder_type,
             check_text(t.bike_type)           AS bike_type
      FROM   trips_2021_q4 AS t

),
check_texts_processed(trip_id, trip_route_category, passholder_type, bike_type) AS(
      SELECT *
      FROM   check_texts AS c
            EXCEPT   
      SELECT *
      FROM   check_texts AS c
      WHERE  c.passholder_type = 'Testing'            
),
result_clear AS(
SELECT ci.trip_id, cd.duration, cda.start_time, cda.end_time, cs.start_station,
       cl.start_lat, cl.start_lon, cs.end_station, cl.end_lat, cl.end_lon,
       cb.bike_id, cp.plan_duration, ct.trip_route_category, ct.passholder_type,
       ct.bike_type

FROM check_durations AS cd 
INNER JOIN check_ids AS ci ON cd.trip_id = ci.trip_id
INNER JOIN check_dates AS cda ON cd.trip_id = cda.trip_id
INNER JOIN check_stations AS cs ON cd.trip_id = cs.trip_id
INNER JOIN check_bike_ids AS cb ON cd.trip_id = cb.trip_id
INNER JOIN check_lat_lons_processed AS cl ON cd.trip_id = cl.trip_id
INNER JOIN check_plan_durations AS cp ON cd.trip_id = cp.trip_id
INNER JOIN check_texts_processed AS ct ON cd.trip_id = ct.trip_id
)
SELECT r.*
FROM  result_clear AS r

SELECT e.*
FROM   edit_t_21_q4 AS e
LIMIT 313;
