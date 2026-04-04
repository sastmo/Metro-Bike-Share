
DROP TABLE IF EXISTS Population_zip_code;
CREATE TABLE Population_zip_code (
  zip_code               integer PRIMARY KEY,
  total_population       integer,
  median_age             decimal,
  total_males            integer,
  total_females          integer,
  total_households       integer,
  average_household_Size decimal
);

\copy Population_zip_code FROM '/Users/admin/OneDrive/Desktop/Dataset/2010_census_populations_zip_code.csv' WITH (FORMAT csv, HEADER TRUE);

TABLE Population_zip_code;



DROP VIEW IF EXISTS Population_by_zip_code CASCADE;
CREATE OR REPLACE VIEW Population_by_zip_code AS 

 SELECT  'California' AS state, 'Los Angeles County' AS city,
         p.zip_code, p.total_population, p.median_age, 
 FROM    Population_zip_code AS p


DROP TABLE IF EXISTS zip_code_location;
CREATE TABLE zip_code_location (
  zip_code      int PRIMARY KEY,
  latitude      decimal,
  longitude     decimal
  );

\copy zip_code_location FROM '/Users/admin/OneDrive/Desktop/Dataset/Us_zip_codes.csv' WITH (FORMAT csv, HEADER TRUE);

TABLE zip_code_location;


DROP TABLE IF EXISTS zip_code_land;
CREATE TABLE zip_code_land (
  ZIP_code           int,
  Land_area_Sqm    decimal
  );

\copy zip_code_land FROM '/Users/admin/OneDrive/Desktop/Dataset/Zipcode_land_area.csv' WITH (FORMAT csv, HEADER TRUE);

TABLE zip_code_land;


