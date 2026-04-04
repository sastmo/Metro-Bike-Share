DROP VIEW IF EXISTS t_22 CASCADE;
CREATE OR REPLACE VIEW t_22 AS 

WITH

 edit_t_22 AS(
  SELECT e.*
  FROM    edit_t_22_q1 AS e
     UNION ALL 
  SELECT e.*
  FROM    edit_t_22_q2 AS e
      UNION ALL 
  SELECT e.*
  FROM    edit_t_22_q3 AS e 
 ),

 duration_class (duration, class, mean_time, mean_frequency) AS(
  SELECT d.duration, d.class, 
         round(d.mean[0]) AS mean_time,  round(d.mean[1]) AS mean_frequency
  FROM   duration_classification_22 AS d
 ),

 edit_start AS (
  SELECT e.trip_id, e.duration,
         CASE d.class 
             WHEN 1 THEN             'long_trip_low_frequency'
             WHEN 2 THEN             'long_trip_mid_frequency'
             WHEN 3 THEN           'short_trip_high_frequency'
             ELSE        'very_short_trip_very_high_frequency'
         END  AS duration_class,
         e.start_time,
         e.start_station, e.start_lat, e.start_lon
  FROM   edit_t_22 AS e, duration_class AS d
  WHERE  e.duration = d.duration 
 ),

 start_detail AS (
  SELECT et.trip_id, et.duration, et.duration_class,
         et.start_time::date AS date_start,
         EXTRACT(YEAR FROM et.start_time) AS start_year,
         EXTRACT(QUARTER FROM et.start_time) AS start_quarter,
         EXTRACT(MONTH FROM et.start_time) AS start_month,
         et.start_time::time AS time_start,
         es.region AS region_start, et.start_station,
         et.start_lat, et.start_lon 

  FROM   edit_start AS et, edited_stations_2022 AS es
  WHERE  et.start_station = es.station_id
 ),

 end_detail AS (
  SELECT et.trip_id,
         et.end_time::date AS date_end, 
         EXTRACT(YEAR FROM et.end_time) AS end_year,
         EXTRACT(QUARTER FROM et.end_time) AS end_quarter,
         EXTRACT(MONTH FROM et.end_time) AS end_month,           
         et.end_time::time AS time_end,
         es.region AS region_end, et.end_station, 
         et.end_lat, et.end_lon,
         et.trip_route_category, et.passholder_type,
         et.bike_id, et.bike_type,
         es.go_live_date AS station_start_date, es.status

  FROM   edit_t_22 AS et, edited_stations_2022 AS es
  WHERE  et.end_station = es.station_id
 ),

 start_end_detail AS ( 
  SELECT s.*, e.date_end, e.end_year, e.end_quarter, e.end_month, e.time_end,
         e.region_end, e.end_station, e.end_lat, e.end_lon, e.trip_route_category,
         e.passholder_type, e.bike_id, e.bike_type,
         e.station_start_date, e.status
  FROM   start_detail AS s
  JOIN   end_detail   AS e   ON s.trip_id = e.trip_id
 ),

 stations_class (start_station, class, mean) AS(
  SELECT s.start_station, s.class, round(s.mean[0]) AS mean
  FROM   station_classification_22 AS s
 ),

 result AS (
  SELECT sed.*, 
         CASE sc.class 
             WHEN 1 THEN   'high_trafic'
             WHEN 2 THEN    'mid_trafic'
             WHEN 3 THEN    'low_trafic'
             ELSE      'very_low_trafic'
         END  AS start_station_class
         --,sc.mean AS AVG_trip_per_class 
  FROM   start_end_detail AS sed, stations_class AS sc
  WHERE  sed.start_station = sc.start_station
 ),
 cube_test AS (
  SELECT   r.start_month,
           --r.duration, r.duration_class,
           --r.region_start,
           --r.start_station,
           r.start_station_class,
           r.duration_class,
           --r.trip_route_category,
           --r.passholder_type, r.bike_id, r.bike_type,
           COUNT (*) AS count_number
  FROM     result AS r
  GROUP BY CUBE(r.start_month,
                -- r.duration, r.region_start,
                r.duration_class, 
                --r.start_station, 
                r.start_station_class
                --,r.trip_route_category, r.passholder_type,
                --r.bike_id, r.bike_type
                ) 
 )
 
 SELECT r.*
 FROM   result AS r

 SELECT ct.*
 FROM   cube_test AS ct
   WHERE    ct.start_month            IS  NULL
   --AND    ct.duration               IS NULL
   --AND    ct.duration_class         IS NOT NULL
   --AND    ct.region_start           IS NOT NULL
  -- AND    ct.start_station          IS NOT NULL
     AND    ct.start_station_class    IS NOT NULL
     AND    ct.duration_class         IS NOT NULL
   --AND    ct.trip_route_category    IS NULL
   --AND    ct.passholder_type        IS NULL
   --AND    ct.bike_id                IS NULL
   --AND    ct.bike_type              IS NULL
     AND    ct.duration_class = 'very_short_trip_very_high_frequency'

 ORDER BY ct.start_month, ct.count_number DESC 




