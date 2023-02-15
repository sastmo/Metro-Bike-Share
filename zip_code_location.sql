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