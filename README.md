![Data Model](https://img.shields.io/badge/Data_Model-Information-ff69b4)
![Data Analysis](https://img.shields.io/badge/Data_Analysis-Analysis-9cf)
![Data Visualization](https://img.shields.io/badge/Data_Visualization-Visualization-yellow)
![Tableau](https://img.shields.io/badge/Tableau-Tools-orange)
![SQL](https://img.shields.io/badge/SQL-Database-blueviolet)
![PL/SQL](https://img.shields.io/badge/PL_SQL-Programming-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-ff4500)


# Metro Bike Share Insights: Navigating Data, Discovery, and Urban Mobility

## 🚀 Welcome to the Metro Bike Share Odyssey on GitHub!
![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/f87dcf6e-b35a-4442-b1d2-02334a3016bb)


Dive into the vibrant world of Los Angeles' bike-sharing system with the Metro Bike Share Insights project. This README guides you through our exploration of urban mobility data. The journey is divided into 6 insightful sections:

1. **Data Model**
2. **Daily and Seasonality Analysis**
3. **Trips Analysis**
4. **Exploring and Analyzing Fleet**
5. **Station Insights and Urban Dynamics-1**
6. **Station Insights and Urban Dynamics-2**

Let's begin!

---

## 🌐 1. Data Model: Crafting the Foundation
![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/e030c7b9-96b1-4ff6-9469-09a9698c2011)


Our journey starts with constructing a robust data model using Star Schema, data cleaning, and format unifying. We employed Common Table Expressions for efficient data handling and K-means clustering for insightful bike-sharing pattern analysis.

**GitHub Navigation:**

🛠️This project analyzes approximately 1 million trips from the beginning of 2019 to the third quarter of 2022 in the Los Angeles Metro Bike Share system, visualized using Tableau and connected through PostgreSQL. Below is a brief guide to navigating through the repository:

**1️⃣ Importing and Cleaning:**

- **File**: **`metro_bike_share.sql`**
- **Details**: Contains SQL codes for importing and cleaning trip and station data. Separate files for each quarter are provided.

**2️⃣ Data Processing:**

- **Station Classification**: **`Processing_station_class.sql`** with year suffix.
- **Trip Duration Classification**: **`Processing_duration_class.sql`** with year suffix.

**3️⃣ Data Preparation for Analysis:**

- **File**: **`Preparation_Analysis.sql`**
- **Details**: Joins edited trip and station tables, includes previous classifications, and creates VIEWs for each year (2019-2022) for Tableau export.

**4️⃣ Demographic Data:**

- **File**: **`Population_zip_code.sql`**
- **Details**: Queries for adding demographic data.

**5️⃣ Public Transportation Station Data:**

- **File**: **`Public_transportation_stations.sql`**
- **Details**: Queries for importing public transportation data in JSON format.

[🔗 Data Model Detailed Overview](https://www.notion.so/1-Data-Model-2b1381a463244351ba7639e905ddcb0c?pvs=4)

---

## 🚴‍♂️ 2. Daily and Seasonality Analysis: Unveiling the Pulse of the City

![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/9a634896-7444-43bf-b9dd-d548b826936e)

This section explores the impact of the pandemic on bike-sharing and uncovers the daily and monthly trip patterns. It's an analysis of how seasonality and external influences shape Metro Bike Share usage.

[🔗 Daily and Seasonality Analysis Full Insights](https://www.notion.so/Daily-and-Seasonality-Analysis-321469023c4d45fc996e7a28651c3f92?pvs=4)

---

## 🔍 3. Trips Analysis: Deciphering the Why Behind Each Ride

![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/d60be258-ed9f-4107-8076-a85b7640072a)

We delve into trip categories and purposes, employing SQL and K-means to understand rider behaviors and preferences. This analysis is key to rethinking strategies for pricing, availability, and customer engagement.

[🔗 Detailed Trips Analysis](https://www.notion.so/Trips-Analysis-efa31fa491f84fd1b387c9d7deaaa80a?pvs=4)

---

## 🚲 4. Exploring and Analyzing Fleet: The Backbone of the Program

![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/0a16c72d-e7cc-4c14-967e-410b3d9ba5b5)

Focusing on the fleet, we analyze the roles of different bike types in the program. This chapter provides insights into resource utilization and system performance.

[🔗 Fleet Analysis Overview](https://www.notion.so/Exploring-and-Analyzing-Fleet-Uncovering-Insights-and-Trends-3b8ae9504fed413fbb59b4560b274d81?pvs=4)

---

## 🌆 5. Station Insights and Urban Dynamics-1: Mapping the Landscape

![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/758df468-d9c8-4fde-9e80-a33ce4ca08b2)

We zoom into the bike stations, analyzing their interplay with public transportation and demographics. This section reveals user behaviors and preferences.

[🔗 Station Insights and Urban Dynamics-1](https://www.notion.so/Station-Insights-and-Urban-Dynamics-1-c10626aec2514683b745fd3bbc6a825b?pvs=4)

---

## 🌍 6. Station Insights and Urban Dynamics-2: Deepening the Exploration

![image](https://github.com/sastmo/Metro-Bike-Share/assets/116411251/24b9bc81-afb7-4eed-b067-bb99414750bd)

We connect demographic factors, public transportation, and station characteristics to bike-sharing usage, unveiling urban mobility's evolving nature.

[🔗 Station Insights and Urban Dynamics-2](https://www.notion.so/Station-Insights-and-Urban-Dynamics-2-298a6c6510814458b4986be617498a78?pvs=4)

---

## 🔜 What's Next?

We're gearing up for advanced statistical analyses and predictive modeling to optimize the Metro Bike Share program for Los Angeles' dynamic landscape.

---

**Join us on this data-driven adventure and discover the story of urban mobility in Los Angeles. Stay tuned for more insights and updates!** 🚴‍♀️🌟🔍
