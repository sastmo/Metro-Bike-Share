--NOTE Import TABALES FROM 2019-q1 TO 2022-q3 

DROP TYPE IF EXISTS trips CASCADE;
CREATE TYPE trips AS (
  trip_id        text,-- Locally unique integer that identifies the trip
  duration       text,-- Length of trip in minutes
  start_time     text,-- The date/time when the trip began
  end_time       text,-- The date/time when the trip ended
  start_station  text,-- The station ID where the trip originated
  start_lat      text,-- The latitude of the station where the trip originated  [north pole +90°..-90° south pole]
  start_lon      text,-- The longitude of the station where the trip originated [east +180°..-180° west of meridia]
  end_station    text,-- The station ID where the trip terminated
  end_lat        text,-- The latitude of the station where the trip terminated
  end_lon        text,-- The longitude of the station where the trip terminated
  bike_id        text,-- Locally unique integer that identifies the bike
  plan_duration  text,-- The number of days that the plan the passholder is using entitles them to ride; 0 is used for a single ride plan (Walk-up)
  trip_route_category text,-- "Round Trip" for trips starting and ending at the same station or "One Way" for all other trips
  passholder_type     text,-- The name of the passholder's plan
  bike_type           text -- The kind of bike used on the trip, including standard pedal-powered bikes, electric assist bikes, or smart bikes         
);

DROP TYPE IF EXISTS stations CASCADE;
CREATE TYPE stations AS (
  Station_ID     text,-- Unique integer that identifies the station
  Station_Name   text,-- The public name of the station. "Virtual Station" is used by staff to check in or check out a bike remotely for
                      -- a special event or in a situation in which a bike could not otherwise be checked in or out to a station.
  Go_live_date   text,-- The date that the station was first available
  Region         text,-- he municipality or area where a station is located, includes DTLA (Downtown LA), Pasadena, Port of LA, Venice
  Status         text-- "Active" for stations available or "Inactive" for stations that are not available as of the latest update    
);


-- Import metro-trips-2019-q1

DROP TABLE IF EXISTS trips_2019_q1 CASCADE;
CREATE TABLE trips_2019_q1 OF trips;
ALTER TABLE  trips_2019_q1 ADD PRIMARY KEY (trip_id);
\copy trips_2019_q1 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2019-q1.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2019-q2

DROP TABLE IF EXISTS trips_2019_q2 CASCADE;
CREATE TABLE trips_2019_q2 OF trips;
ALTER TABLE  trips_2019_q2 ADD PRIMARY KEY (trip_id);
\copy trips_2019_q2 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2019-q2.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2019-q3

DROP TABLE IF EXISTS trips_2019_q3 CASCADE;
CREATE TABLE trips_2019_q3 OF trips;
ALTER TABLE  trips_2019_q3 ADD PRIMARY KEY (trip_id);
\copy trips_2019_q3 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2019-q3.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2019-q4

DROP TABLE IF EXISTS trips_2019_q4 CASCADE;
CREATE TABLE trips_2019_q4 OF trips;
ALTER TABLE  trips_2019_q4 ADD PRIMARY KEY (trip_id);
\copy trips_2019_q4 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2019-q4.csv' WITH (FORMAT csv, HEADER TRUE);
/
-- Import metro-trips-2020-q1

DROP TABLE IF EXISTS trips_2020_q1 CASCADE;
CREATE TABLE trips_2020_q1 OF trips;
ALTER TABLE  trips_2020_q1 ADD PRIMARY KEY (trip_id);
\copy trips_2020_q1 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2020-q1.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2020-q2

DROP TABLE IF EXISTS trips_2020_q2 CASCADE;
CREATE TABLE trips_2020_q2 OF trips;
ALTER TABLE  trips_2020_q2 ADD PRIMARY KEY (trip_id);
\copy trips_2020_q2 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2020-q2.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2020-q3

DROP TABLE IF EXISTS trips_2020_q3 CASCADE;
CREATE TABLE trips_2020_q3 OF trips;
ALTER TABLE  trips_2020_q3 ADD PRIMARY KEY (trip_id);
\copy trips_2020_q3 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2020-q3.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2020-q4

DROP TABLE IF EXISTS trips_2020_q4 CASCADE;
CREATE TABLE trips_2020_q4 OF trips;
ALTER TABLE  trips_2020_q4 ADD PRIMARY KEY (trip_id);
\copy trips_2020_q4 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2020-q4.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2021-q1

DROP TABLE IF EXISTS trips_2021_q1 CASCADE;
CREATE TABLE trips_2021_q1 OF trips;
ALTER TABLE  trips_2021_q1 ADD PRIMARY KEY (trip_id);
\copy trips_2021_q1 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2021-q1.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2021-q2

DROP TABLE IF EXISTS trips_2021_q2 CASCADE;
CREATE TABLE trips_2021_q2 OF trips;
ALTER TABLE  trips_2021_q2 ADD PRIMARY KEY (trip_id);
\copy trips_2021_q2 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2021-q2.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2021-q3

DROP TABLE IF EXISTS trips_2021_q3 CASCADE;
CREATE TABLE trips_2021_q3 OF trips;
ALTER TABLE  trips_2021_q3 ADD PRIMARY KEY (trip_id);
\copy trips_2021_q3 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2021-q3.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2021-q4

DROP TABLE IF EXISTS trips_2021_q4 CASCADE;
CREATE TABLE trips_2021_q4 OF trips;
ALTER TABLE  trips_2021_q4 ADD PRIMARY KEY (trip_id);
\copy trips_2021_q4 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2021-q4.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2022-q1

DROP TABLE IF EXISTS trips_2022_q1 CASCADE;
CREATE TABLE trips_2022_q1 OF trips;
ALTER TABLE  trips_2022_q1 ADD PRIMARY KEY (trip_id);
\copy trips_2022_q1 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2022-q1.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2022-q2

DROP TABLE IF EXISTS trips_2022_q2 CASCADE;
CREATE TABLE trips_2022_q2 OF trips;
ALTER TABLE trips_2022_q2 ADD PRIMARY KEY (trip_id);
\copy trips_2022_q2 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2022-q2.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-trips-2022-q3

DROP TABLE IF EXISTS trips_2022_q3 CASCADE;
CREATE TABLE trips_2022_q3 OF trips;
ALTER TABLE  trips_2022_q3 ADD PRIMARY KEY (trip_id);
\copy trips_2022_q3 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-trips-2022-q3.csv' WITH (FORMAT csv, HEADER TRUE);

-- Import metro-stations-2021
--All of the stations data exist in the last version of the station table,
--so the last version has been used only

DROP TABLE IF EXISTS  metro_stations_2022 CASCADE;
CREATE TABLE metro_stations_2022 OF stations;
ALTER TABLE  metro_stations_2022 ADD PRIMARY KEY (Station_ID);
\copy metro_stations_2022 FROM '/Users/admin/OneDrive/Desktop/Dataset/metro-bike-stations-2022.csv' WITH (FORMAT csv, HEADER TRUE);

----------------------- Check-id Function------------------
--Chek the id format and add passed data to the edit table as integer 
DROP FUNCTION IF EXISTS is_integer(text) CASCADE;
CREATE FUNCTION is_integer(n text) RETURNS boolean AS
$$
BEGIN
  IF  (SELECT string_agg(t.match[1], '') ~ n AS "pattern?"
       FROM   regexp_matches(n,'([0-9]+)','g') AS t(match))
        THEN 
             RETURN TRUE;
    ELSE 
      RETURN FALSE;
  END IF;           
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS check_id(text);
CREATE FUNCTION check_id(t_id text) RETURNS integer AS
$$
DECLARE edited_id integer;
BEGIN
      CASE WHEN is_integer(t_id)
           THEN edited_id := t_id::integer;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
           edited_id := '1'::integer;
  END CASE;
      RETURN edited_id;
END;
$$
LANGUAGE PLPGSQL;

----------------------- Check_date Function------------------
--Chek the date and time format and add passed data to the edit table as timestamp

DROP FUNCTION IF EXISTS is_date(text) CASCADE;
CREATE FUNCTION is_date(s text) RETURNS boolean AS
$$
BEGIN
PERFORM s::timestamp;
  RETURN TRUE;
EXCEPTION WHEN others THEN
   RETURN FALSE;
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS Check_date(text) CASCADE;
CREATE FUNCTION Check_date(t_time text) RETURNS timestamp AS
$$
DECLARE edited_time timestamp;
BEGIN
      CASE WHEN is_date(t_time)
           THEN edited_time := t_time::timestamp;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
           edited_time := '1111-11-11 11:11:11'::timestamp;
  END CASE;
      RETURN edited_time;
END;
$$
LANGUAGE PLPGSQL;
--------------------Check_duration Function--------------------

DROP FUNCTION IF EXISTS check_duration(text) CASCADE;
CREATE FUNCTION check_duration(Δt text) RETURNS interval MINUTE AS
$$
DECLARE edited_Δt interval;
BEGIN
      CASE WHEN is_integer(Δt)
           THEN edited_Δt :=(Δt ||'minutes')::interval;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
          edited_Δt :='1111111111 minutes'::interval;
  END CASE;
      RETURN edited_Δt;
END;
$$
LANGUAGE PLPGSQL;
---------------------Check_station Function---------------------

DROP FUNCTION IF EXISTS check_station(text) CASCADE;
CREATE FUNCTION check_station(station text) RETURNS integer AS
$$
DECLARE edited_station integer;
BEGIN
      CASE WHEN is_integer(station)
           THEN edited_station := station::integer;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
          edited_station :='1'::integer;
  END CASE;
      RETURN edited_station;
END;
$$
LANGUAGE PLPGSQL;
---------------------Check_bike_id Function---------------------

DROP FUNCTION IF EXISTS correct_b_id(text, text);
CREATE FUNCTION correct_b_id(t_id text, b_id text) RETURNS integer AS 
$$
DECLARE
    bik_ids  RECORD;
    id      integer;
    i   integer :=1;
BEGIN
--------- Analysis to find max number of each bike at the specific station
----------and specific type and used for each trip type
CREATE temp TABLE selected_bikes ON COMMIT DROP AS
WITH 
 foult_recod AS (
 SELECT   t.*
 FROM     trips_2019_q1 AS t
 WHERE    t.trip_id = t_id
 ),
 cube_test  AS  (
  SELECT t.trip_route_category,
         t.passholder_type,
         t.bike_type,
         t.start_station,
         COUNT(t.bike_id) AS numbers,
         string_agg(t.bike_id, ', ') AS bike_ids,
         t.bike_id AS bike_id
  FROM   trips_2019_q1 AS t
  GROUP BY CUBE (trip_route_category, bike_id,
                 passholder_type, bike_type, start_station)
),
test_1 (trip_route_category, bike_type, start_station, numbers, bike_id ) AS (
SELECT  ct.trip_route_category,-- ct.passholder_type, 
        ct.bike_type, ct.start_station, ct.numbers AS numbers,
        ct.bike_id
 FROM   cube_test AS ct, foult_recod AS f
 WHERE  ct.start_station       IS NOT NUll 
 AND    ct.trip_route_category IS NOT NULL 
 AND    ct.passholder_type     IS NULL 
 AND    ct.bike_type           IS NOT NULL
 AND    ct.bike_id             IS NOT NULL
 AND    ct.start_station = f.start_station
 AND    ct.bike_type = f.bike_type
 ORDER BY  numbers DESC
),
Highest_frequency (rank_order, bike_id, numbers) AS(
SELECT ROW_NUMBER() OVER(ORDER BY numbers DESC), t.bike_id, t.numbers 
FROM   test_1 AS t
),
selected_bike(rank_order, bike_id, start_time, end_time) AS(
SELECT h.rank_order, h.bike_id, f.start_time, f.end_time
FROM   Highest_frequency AS h,
       foult_recod AS f
) 
SELECT * FROM selected_bike;
----------------- Loop to check inervals and check wheather at the 
-----------------time which missed bike-id had been used 
FOR bik_ids IN
     SELECT s.rank_order, s.bike_id, s.start_time, s.end_time
     FROM   selected_bikes AS s
   LOOP 
   id := (SELECT s.bike_id::integer 
          FROM selected_bikes AS s 
          WHERE  rank_order = i LIMIT 1);
   IF 
      ( SELECT CASE WHEN (s.start_time::timestamp > t.end_time::timestamp) OR 
                         (s.end_time::timestamp   < t.start_time::timestamp)
                    THEN true
                    ELSE false
                END CASE         
        FROM    selected_bikes AS s, trips_2019_q1 AS t
        WHERE    id::text = t.bike_id
        AND      id::text = s.bike_id
        LIMIT 1
      )
    THEN
        RETURN id;
        EXIT;
    ELSE
        i=i+1;
        CONTINUE;
    END IF;
   END LOOP;
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS check_b_ids (text, text);
CREATE FUNCTION check_b_ids(t_id text, b_id text) RETURNS integer AS
$$
DECLARE edited_id integer;
BEGIN
      CASE WHEN is_integer(b_id)
           THEN edited_id := b_id::integer;           
       ELSE 
         edited_id := correct_b_id(t_id, b_id);
         edited_id := 1;
  END CASE;
      RETURN edited_id;
END;
$$
LANGUAGE PLPGSQL;

----------------------check_lat/lon----------------------------

DROP FUNCTION IF EXISTS is_location(text) CASCADE;
CREATE FUNCTION is_location(loc text) RETURNS boolean AS
$$
BEGIN
  IF  (SELECT string_agg(t.match[1], '') ~ loc AS "pattern?"
       FROM   regexp_matches(loc,'(([\-+])?[0-9]{1,3}\.[0-9]*)','g') AS t(match))
        THEN 
             RETURN TRUE;
    ELSE 
      RETURN FALSE;
  END IF;           
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS check_lat_lon(text) CASCADE;
CREATE FUNCTION check_lat_lon(loc text) RETURNS decimal AS
$$
DECLARE edited_loc decimal;
BEGIN
      CASE WHEN is_location(loc)
           THEN edited_loc := loc::decimal;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
          edited_loc := '0'::decimal;
  END CASE;
      RETURN edited_loc;
END;
$$
LANGUAGE PLPGSQL;

--------------------------check_plan_duration------------------------

DROP FUNCTION IF EXISTS check_plan_duration(text) CASCADE;
CREATE FUNCTION check_plan_duration(plane_day text) RETURNS integer AS
$$
DECLARE edited_plane_day integer;
BEGIN
      CASE WHEN is_integer(plane_day)
           THEN edited_plane_day := plane_day::decimal;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_time := Correct_fucntion() ;
          edited_plane_day := '1111'::integer;
  END CASE;
      RETURN edited_plane_day;
END;
$$
LANGUAGE PLPGSQL;
-----------------------------Check_text---------------------------------
DROP FUNCTION IF EXISTS is_text(text) CASCADE;
CREATE FUNCTION is_text(tex text) RETURNS boolean AS
$$
BEGIN
  IF  (SELECT string_agg(t.match[1], '') ~ tex AS "pattern?"
       FROM   regexp_matches(tex,'([a-z]+[\s+\-]?[a-z]*[\s+]?[a-z]*)','gi') AS t(match)) 
       AND tex <>'null'
        THEN 
             RETURN TRUE;
    ELSE 
      RETURN FALSE;
  END IF;           
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS check_text(text) CASCADE;
CREATE FUNCTION check_text(tex text) RETURNS text AS
$$
DECLARE edited_tex text;
BEGIN
      CASE WHEN is_text(tex)  -- Make format uniqe for all the texts  
           THEN CASE WHEN tex ~ '-'
                     THEN edited_tex := INITCAP(SPLIT_PART(tex, '-',1)) ||'-'
                                            || LOWER(SPLIT_PART(tex, '-',2));
                     ELSE edited_tex := INITCAP(tex);
                END CASE;
       ELSE 
          --Correct function if required   
          --edited_time := Correct_fucntion() ;
          edited_tex := '0'::text;
  END CASE;
      RETURN edited_tex;
END;
$$
LANGUAGE PLPGSQL;

----------------------------------CLearing --------------------------
DROP TYPE IF EXISTS mbs_trips CASCADE;
CREATE TYPE mbs_trips AS (
  trip_id              int,             -- Locally unique integer that identifies the trip
  duration             interval MINUTE, -- Length of trip in minutes
  start_time           timestamp ,      -- The date/time when the trip began
  end_time             timestamp ,      -- The date/time when the trip ended
  start_station        int,             -- The station ID where the trip originated
  start_lat            decimal,         -- The latitude of the station where the trip originated  [north pole +90°..-90° south pole]
  start_lon            decimal,         -- The longitude of the station where the trip originated [east +180°..-180° west of meridia]
  end_station          int,             -- The station ID where the trip terminated
  end_lat              decimal,         -- The latitude of the station where the trip terminated
  end_lon              decimal,         -- The longitude of the station where the trip terminated
  bike_id              int,             -- Locally unique integer that identifies the bike
  plan_duration        int,            -- The number of days that the plan the passholder is using entitles them to ride; 0 is used for a single ride plan (Walk-up)
  trip_route_category  text,            -- "Round Trip" for trips starting and ending at the same station or "One Way" for all other trips
  passholder_type      text,            -- The name of the passholder's plan
  bike_type            text             -- The kind of bike used on the trip, including standard pedal-powered bikes, electric assist bikes, or smart bikes         
);

--The edit_table to gather data with the correct format and converted type 
DROP TABLE IF EXISTS edit_t_19_q1 CASCADE;  
CREATE TABLE edit_t_19_q1 OF mbs_trips; 

INSERT INTO edit_t_19_q1
WITH

 check_ids(trip_id) AS(
    SELECT check_id(t.trip_id)
    FROM   trips_2019_q1 AS t
),
  check_durations(trip_id, duration) AS(
    SELECT t.trip_id::int, 
           check_duration(t.duration) AS duration
    FROM   trips_2019_q1 AS t
),
  check_dates (trip_id, start_time, end_time) AS (
    SELECT t.trip_id::int,
           Check_date(t.start_time) AS start_time,
           Check_date(t.end_time  ) AS end_time
    FROM   trips_2019_q1 AS t
), 
 check_stations(trip_id, start_station, end_station) AS(
    SELECT t.trip_id::int,
           check_station(t.start_station) AS start_station,
           check_station(t.end_station  ) AS end_time
    FROM   trips_2019_q1 AS t
),
 check_bike_ids(trip_id, bike_id) AS(
    SELECT t.trip_id::int,
           check_b_ids(t.trip_id, t.bike_id) AS bike_id
    FROM   trips_2019_q1 AS t
),
check_lat_lons(trip_id, start_lat, start_lon, end_lat, end_lon) AS(
     SELECT t.trip_id::int,
            check_lat_lon(t.start_lat) AS start_lat, 
            check_lat_lon(t.start_lon) AS start_lon,
            check_lat_lon(t.end_lat  ) AS end_lat, 
            check_lat_lon(t.end_lon  ) AS end_lon            
     FROM   trips_2019_q1 AS t    
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
      FROM   trips_2019_q1 AS t
              EXCEPT 
      SELECT t.trip_id:: int,
             check_plan_duration(t.plan_duration) AS plan_duration 
      FROM   trips_2019_q1 AS t
      WHERE  t.plan_duration = '999'  -- plan_duration = '999' is for testing
                                      -- smart bike at Virtual station
),
check_texts(trip_id, trip_route_category, passholder_type, bike_type) AS(
      SELECT t.trip_id::int,
             check_text(t.trip_route_category) AS trip_route_category,
             check_text(t.passholder_type)     AS passholder_type,
             check_text(t.bike_type)           AS bike_type
      FROM   trips_2019_q1 AS t

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

-------------------------------Station Table Clearing-------------------------------------
----------------------- Check_date Fuction------------------
--Chek the date format and add passed data to the edit table as date

DROP FUNCTION IF EXISTS is_dates(text) CASCADE;
CREATE FUNCTION is_dates(s text) RETURNS boolean AS
$$
BEGIN
PERFORM s::date;
  RETURN TRUE;
EXCEPTION WHEN others THEN
   RETURN FALSE;
END;
$$ 
LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS valid_date(text) CASCADE;
CREATE FUNCTION valid_date(t_date text) RETURNS date AS
$$
DECLARE edited_date date;
BEGIN
      CASE WHEN is_dates(t_date)
           THEN edited_date := t_date::date;           
       ELSE 
          --Correct function if required (We have not found any false data)   
          --edited_date := Correct_fucntion() ;
           edited_date := '1111-11-11'::date;
  END CASE;
      RETURN edited_date;
END;
$$
LANGUAGE PLPGSQL;
---------------------------Edit station table------------------------------

DROP TYPE IF EXISTS station CASCADE;
CREATE TYPE station AS (
  Station_ID     int,-- Unique integer that identifies the station
  Station_Name   text,-- The public name of the station. "Virtual Station" is used by staff to check in or check out a bike remotely for
                      -- a special event or in a situation in which a bike could not otherwise be checked in or out to a station.
  Go_live_date   date,-- The date that the station was first available
  Region         text,-- he municipality or area where a station is located, includes DTLA (Downtown LA), Pasadena, Port of LA, Venice
  Status         text-- "Active" for stations available or "Inactive" for stations that are not available as of the latest update    
);

TABLE metro_stations_2022;

DROP TABLE IF EXISTS  edited_stations_2022 CASCADE;
CREATE TABLE edited_stations_2022 OF station;
ALTER TABLE  edited_stations_2022 ADD PRIMARY KEY (Station_ID);

INSERT INTO edited_stations_2022
INSERT INTO testtii
WITH 
 check_stations_id(station_id, station_name) AS(
    SELECT check_station(m.station_id) AS station_id,
           m.station_name
    FROM   metro_stations_2022 AS m
        EXCEPT
    SELECT check_station(m.station_id) AS station_id,
           m.station_name
    FROM   metro_stations_2022 AS m
    WHERE  m.station_name = 'Virtual Station'  -- We do't want to consider this three station in our analysis
       OR  m.station_name = 'Metro Bike Share Free Bikes'
       OR  m.station_name = 'Metro Bike Share Out of Service Area Smart Bike'
),
  check_dates(station_id, go_live_date, region, status) AS (
    SELECT check_station(m.station_id) AS station_id,
           valid_date(m.go_live_date) AS go_live_date,
           m.region, m.status
    FROM   metro_stations_2022 AS m
),
result_clear AS(
    SELECT c.station_id, c.station_name,
           cs.go_live_date, cs.region, cs.status
    FROM   check_stations_id AS c
    INNER JOIN check_dates AS cs ON cs.station_id= c.station_id
)
SELECT r.*
FROM   result_clear AS r;

-------------------station's location and zipe code--------------
DROP TABLE IF EXISTS station_zipcode CASCADE;
CREATE TABLE station_zipcode (
  Station_ID     int ,
  latitude       decimal,
  longitude      decimal,
  zip_code       int,
  state          text
);

INSERT INTO station_zipcode
WITH 
 locations AS(
  SELECT DISTINCT ON (t.start_station) t.start_station,
                      t.start_lat, t.start_lon
  FROM   trip_shared_bike AS t
      UNION
  SELECT DISTINCT ON (t.end_station) t.end_station,
                     t.end_lat, t.end_lon
  FROM   trip_shared_bike AS t
 ),
 zip_codes AS (
    SELECT l.*, 
      -- The geolocation with the closest proximity
      -- from different data sets is taken into account
      -- within the same zip code. 
        (SELECT z.zip_code      
         FROM   zip_code_location AS z
         ORDER BY point(l.start_lat, l.start_lon)<->point(z.latitude, z.longitude) -- closest proximity
         LIMIT 1), 'California' AS state
    FROM   locations AS l 
 )
 SELECT *
 FROM   zip_codes

 SELECT DISTINCT ON(zip_code) COUNT(*)
 FROM station_zipcode
 GROUP BY zip_code;

