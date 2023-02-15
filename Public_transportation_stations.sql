----------------------------metro_station-----------------------------
DROP TABLE IF EXISTS metro_station CASCADE;
 CREATE TABLE metro_station (
  data text
);

\copy metro_station FROM '/Users/admin/OneDrive/Desktop/Dataset/metro_station.csv';

TABLE metro_station;
 UPDATE metro_station SET data = regexp_replace(data, ':null,', ':0,', 'g');

----Metro stations---------------
DROP TYPE IF EXISTS edited_station CASCADE;
CREATE TYPE edited_station AS (
  station_name   text ,
  city           text,
  state          text,
  latitude       decimal,
  longitude      decimal,
  station_type   text,
  zip_code      int
);

DROP TABLE IF EXISTS edited_metro_station CASCADE;
CREATE TABLE edited_metro_station OF edited_station;

INSERT INTO edited_metro_station
 WITH 

 metro_station_json  AS (      --  
  SELECT m.data::json AS data
  FROM   metro_station AS m
 ),
  unzip_step_1 AS (
  SELECT objs.value
  FROM   json_each((TABLE metro_station_json)) AS objs(o , value)
  WHERE  objs.o = 'features' -- Geolocation data is stored in the 'features' key
 ),
 unzip_step_2 AS (
  SELECT objs.value
  FROM   json_array_elements((TABLE unzip_step_1)) AS objs(value)
 ),
 unzip_step_3 AS (
  SELECT objs.value ::json -> 'attributes' ->> 'Name'      AS station_name,
         objs.value ::json -> 'attributes' ->> 'city'      AS city,
         objs.value ::json -> 'attributes' ->> 'state'     AS state,
         objs.value ::json -> 'attributes' ->> 'latitude'  AS latitude,
         objs.value ::json -> 'attributes' ->> 'longitude' AS longitude

  FROM   unzip_step_2 AS objs(value)
 ),
 result AS (
  SELECT regexp_replace(u.station_name, 'Metro Station', '') AS station_name,
         u.city, 'California' AS state,
         u.latitude :: decimal, u.Longitude :: decimal,
         'metro_station' AS station_type
  FROM   unzip_step_3 AS u 
 ),

 result_zipecode AS (
 SELECT r.*, 
     -- The geolocation with the closest proximity
      -- from different data sets is taken into account
      -- within the same zip code. 
        (SELECT z.zip_code 
         FROM   zip_code_location AS z
         ORDER BY point(r.latitude, r.longitude)<->point(z.latitude, z.longitude)
         LIMIT 1)
 FROM   result AS r  
 )
 SELECT r.*
 FROM   result_zipecode AS r;

TABLE edited_metro_station;

-------------------------------metrolink_station-------------------------------

DROP TABLE IF EXISTS metrolink_station CASCADE;
 CREATE TABLE metrolink_station (
  data text
);

\copy metrolink_station FROM '/Users/admin/OneDrive/Desktop/Dataset/metrolink_station.csv';

TABLE metrolink_station;

 UPDATE metrolink_station SET data = regexp_replace(data, ':null,', ':0,', 'g');

----Cleaning---------------

DROP TABLE IF EXISTS edited_metrolink_station CASCADE;
CREATE TABLE edited_metrolink_station OF edited_station;

INSERT INTO edited_metrolink_station
 WITH 

 metrolink_station_json  AS (
  SELECT m.data::json AS data
  FROM   metrolink_station AS m
 ),
  unzip_step_1 AS (
  SELECT objs.value
  FROM   json_each((TABLE metrolink_station_json)) AS objs(o , value)
  WHERE  objs.o = 'features'
 ),
 unzip_step_2 AS (
  SELECT objs.value
  FROM   json_array_elements((TABLE unzip_step_1)) AS objs(value)
 ),
 unzip_step_3 AS (
  SELECT objs.value ::json -> 'attributes' ->> 'Name' AS station_name,
         objs.value ::json -> 'attributes' ->> 'city'      AS city,
         objs.value ::json -> 'attributes' ->> 'state'     AS state,
         objs.value ::json -> 'attributes' ->> 'latitude'  AS latitude,
         objs.value ::json -> 'attributes' ->> 'longitude' AS longitude

  FROM   unzip_step_2 AS objs(value)
 ),
 result AS (
  SELECT regexp_replace(u.station_name, 'Metrolink Station', '') AS station_name,
         u.city, 'California' AS state,
         u.latitude :: decimal, u.Longitude :: decimal,
         'metrolink_station' AS station_type
  FROM   unzip_step_3 AS u
 ),
 result_zipecode AS (
 SELECT r.*, 
        (SELECT z.zip_code 
         FROM   zip_code_location AS z
         ORDER BY point(r.latitude, r.longitude)<->point(z.latitude, z.longitude)
         LIMIT 1)
 FROM   result AS r  
 )
 SELECT r.*
 FROM   result_zipecode AS r;

TABLE edited_metrolink_station;

-------------------------------amtrak_station-------------------------------

DROP TABLE IF EXISTS amtrak_station CASCADE;
 CREATE TABLE amtrak_station (
  data text
);

\copy amtrak_station FROM '/Users/admin/OneDrive/Desktop/Dataset/amtrak_station.csv';

TABLE amtrak_station;

 UPDATE amtrak_station SET data = regexp_replace(data, ':null,', ':0,', 'g');

----Cleaning---------------

DROP TABLE IF EXISTS edited_amtrak_station CASCADE;
CREATE TABLE edited_amtrak_station OF edited_station;

INSERT INTO edited_amtrak_station
 WITH 

 amtrak_station_json  AS (
  SELECT m.data::json AS data
  FROM   amtrak_station AS m
 ),
  unzip_step_1 AS (
  SELECT objs.value
  FROM   json_each((TABLE amtrak_station_json)) AS objs(o , value)
  WHERE  objs.o = 'features'
 ),
 unzip_step_2 AS (
  SELECT objs.value
  FROM   json_array_elements((TABLE unzip_step_1)) AS objs(value)
 ),
 unzip_step_3 AS (
  SELECT objs.value ::json ->'properties' ->> 'Name'      AS station_name,
         objs.value ::json ->'properties' ->> 'city'      AS city,
         objs.value ::json ->'properties' ->> 'state'     AS state,
         objs.value ::json ->'geometry'   -> 'coordinates' ->> 1 AS latitude,
         objs.value ::json ->'geometry'   -> 'coordinates' ->> 0 AS longitude

  FROM   unzip_step_2 AS objs(value)
 ),
 result AS (
  SELECT SPLIT_PART(u.station_name,',',1) AS station_name, u.city,
         'California' AS state,
         u.latitude :: decimal, u.Longitude :: decimal,
         'amtrak_station' AS station_type

  FROM   unzip_step_3 AS u
 ),
 result_zipecode AS (
 SELECT r.*, 
        (SELECT z.zip_code 
         FROM   zip_code_location AS z
         ORDER BY point(r.latitude, r.longitude)<->point(z.latitude, z.longitude)
         LIMIT 1)
 FROM   result AS r  
 )
 SELECT r.*
 FROM   result_zipecode AS r;

TABLE edited_amtrak_station;

-----------------Create view of stations-----------------------

DROP VIEW IF EXISTS public_transportation_stations CASCADE;
CREATE OR REPLACE VIEW public_transportation_stations AS 

WITH 
 stations AS (
  SELECT *
  FROM   edited_metro_station

   UNION ALL

 SELECT *
 FROM   edited_metrolink_station 

   UNION ALL

 SELECT *
 FrOM   edited_amtrak_station
 
 )
 SELECT *
 FROM   stations



TABLE population_by_zip_code



