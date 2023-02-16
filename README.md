# Metro Bike Share Data Analyst and Data Visualization Project:
Metro Bike Share is a bike-sharing system in the Los Angeles, California, area. The service was launched on July 7, 2016. 
After cleaning, about 1 million trips from the beginning of 2019 to the third quarter of 2022 are visualized in Tableau.
Data is imported into Tableau through a connection to the PostgreSQL server. The process of analysis is briefly described in the following.

# 1- Importing and Cleaning files: 
Data for trips and bike stations have been imported and cleaned using codes stored in metro_bike_share.sql.
In addition, there is a dedicated metro_bike_share.sql file for each quarter with a suffix indicating the year and quarter.
Note>> The cleaning process includes defining required functions and cleaning query sections. 
# 2- Data Processing file:
1- Using K-mean clustering, bike stations are classified based on the number of trips each year. 
Related codes are stored in Processing_station_class.sql with a suffix to show the year. 
2- Using K-mean clustering, trips are classified based on trip duration and the frequency of each duration trip. 
Related codes are stored in Processing_duration_class.sql with a suffix to show the year. 
# 3- Data Preparation for Analysis:
Using a query, the edited trips table and station table have been joined. 
Also, previous classifications were performed for stations, and trips have been added to the tables.
The result of the query have been kept in the four VIEWs called t_19, t_20, t_21 and t_22 for the years 2019, 2020, 2021 and 2022, respectively exporting the data to Tableau for visualization.
Related codes are stored in Preparation_Analysis.sql with a suffix to show the year. 

# 4- Demographic data 
Queroes in the Population_zipe_code.sql has been used to bring all demographic data to the database.
# 5- Public Transportation Station data
Queroes in the Public_transportation_stations.sql has been used to bring all public transportation data in JSON format to the database.
