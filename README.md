# COVID-19 Capstone Project

## Description
The purpose of the project is to combine what we've learned throughout the program.
I took several COVID-19 datasets and joined them to make a useful source of 
information regarding COVID-19 spread globally. I focus on:
- cases
- deaths
- vaccinations

I use 3 time series data sources (see details below) and join them based on countries and dates. 

### Technology Stack

- **Storage:** Amazon S3
- **Running environment:** Amazon EMR
- **Pipeline:** Apache Airflow
- **Packages:** pyspark, omegaconf

I choose Amazon S3 - Amazon EMR combination instead of Amazon Redshift mainly because of these
terms:

- Cost: The project doesn't require a cluster to be open 24x7 like Redshift's.
- Data lake: Don't want to work on a traditional data warehouse structure, highly structured joins
  or staging steps.
- Ability to work with different unstructured data formats and potential for big data analytics are
  the other reasons of my choice.


## Data sources

### Source 1 - COVID-19 Activity

[Data.world](https://data.world/covid-19-data-resource-hub/covid-19-case-counts)

A global time series of case and death data.
This data is sourced from JHU CSSE COVID-19 Data as well as The New York Times.

**Schema**

- *people_positive_cases_count* - Number of people who have tested positive for all times

- *county_name* - The county name (US only)

- *province_state_name* - The state or province name

- *report_date* - The date of the report

- *continent_name* - The continent name

- *data_source_name* - Short description of the data source from which the record is sourced

- *people_death_new_count* - Number of people who have died for each day

- *county_fips_number* - County-level FIPS code, uniquely identifying geographic areas (US only)

- *country_alpha_3_code* - ISO3166-1 value. The three-letter code that represents a country name

- *country_short_name* - The country name

- *country_alpha_2_code* - ISO3166-1 value. The two-letter code that represents a country name
  
- *people_positive_new_cases_count* - Number of people who have tested positive for each day
 
- *people_death_count* - Number of people who have died for all times


full dictionary in [link](https://data.world/covid-19-data-resource-hub/covid-19-case-counts/workspace/data-dictionary)

### Source 2 - Location Population

[Data.world](https://data.world/covid-19-data-resource-hub/covid-19-activity-location-population-table)

This location and population table compliments the COVID-19 Activity dataset. This data is sourced from U.S. Census Bureau, Population Division (2019 estimate), the United Nations, Department of Economic and Social Affairs, Population Division (2019 median), and Statistics Canada (2019 estimate).

**Schema**
- *COUNTRY_SHORT_NAME* - The country name
- *COUNTRY_ALPHA_3_CODE* - ISO3166-1 value. The three-letter code that represents a country name
- *COUNTRY_ALPHA_2_CODE* - ISO3166-1 value. The two-letter code that represents a country name

- *PROVINCE_STATE_NAME* - The state or province name

- *COUNTY_NAME* - The county name (US only)

- *COUNTY_FIPS_NUMBER* - Count-level FIPS code, uniquely identifying geographic areas (US only)

- *GEO_LATITUDE* - Latitude for the geographic entity

- *GEO_LONGITUDE* - Longitude for the geographic entity

- *GEO_REGION_POPULATION_COUNT* - Population for the geographic entity

- *DATA_SOURCE_NAME* - name of data source

full dictionary in [link](https://data.world/covid-19-data-resource-hub/covid-19-activity-location-population-table/workspace/data-dictionary)

### Source 3 - COVID vaccinations

[Github](https://github.com/govex/COVID-19/tree/master/data_tables/vaccine_data/global_data)

This is an hourly ETL aggregating vaccination data from many sources worldwide. 

**Schema**
- *Country_Region* - Country or region name
- *Date* - Data collection date

- *Doses_admin* - "Cumulative number of doses administered. When a vaccine requires multiple doses, each one is counted independently"
- *People_partially_vaccinated* - "Cumulative number of people who received at least one vaccine dose. When the person receives a prescribed second dose, it is not counted twice"
- *People_fully_vaccinated* - Cumulative number of people who received all prescribed doses necessary to be considered fully vaccinated
- *Report_Date_String* - Data reported date


# Project Sources
https://github.com/josephmachado/spark_submit_airflow