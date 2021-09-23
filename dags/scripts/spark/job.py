# pyspark
import argparse
import os

from enum import Enum

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


valid_vaccination_type_condition = (
    (F.col('Vaccine_Type') !='Unknown') &
    (F.col('Vaccine_Type') !='All') &
    (F.col('Vaccine_Type') !='Unassigned')
)

def read_sources(spark, input_loc):
    return (
        spark.read.csv(os.path.join(input_loc, 'activity.csv'), header=True),
        spark.read.csv(os.path.join(input_loc, 'population.csv'), header=True),
        spark.read.csv(os.path.join(input_loc, 'vaccinations_global.csv'), header=True),
        spark.read.csv(os.path.join(input_loc, 'vaccinations_usa_doeses.csv'), header=True),
        spark.read.csv(os.path.join(input_loc, 'vaccinations_usa_people.csv'), header=True),
    )

def _run_dim_continent(df_activity, df_population, output_loc):

    df_continent = df_activity\
        .select('COUNTRY_SHORT_NAME', 'CONTINENT_NAME')\
        .dropna()\
        .dropDuplicates()\
        .persist()\
        .join(df_population, 'COUNTRY_SHORT_NAME').groupBy(
            'CONTINENT_NAME'
    ).agg(
        F.mean('GEO_LATITUDE').alias('geo_latitude'),
        F.mean('GEO_LONGITUDE').alias('geo_longitude'))\
            .withColumnRenamed('CONTINENT_NAME', 'continent_name')

    df_continent.write.parquet(
        os.path.join(output_loc, 'dim_continent'), mode='overwrite')


def _run_dim_data_sources(df_activity, df_population, output_loc):
    df_data_sources = df_activity\
        .select('DATA_SOURCE_NAME')\
        .dropna()\
        .dropDuplicates()\
            .union(
                df_population.select('DATA_SOURCE_NAME')\
                    .dropna()\
                    .dropDuplicates()
            )

    df_data_sources.write.parquet(
        os.path.join(output_loc, 'dim_data_sources'), mode='overwrite')

def _run_dim_vaccinations_types(df_vacc_usa_doses, output_loc):
    df_vacc_types = df_vacc_usa_doses\
        .filter(valid_vaccination_type_condition)\
        .select('Vaccine_Type')\
        .dropna()\
        .dropDuplicates()
    df_vacc_types.write.parquet(
        os.path.join(output_loc, 'dim_vaccinations_types', mode='overwrite'))


def _run_dim_countries(df_activity, df_population, output_loc):
    df_conutries = df_activity\
        .select('COUNTRY_SHORT_NAME', 'CONTINENT_NAME')\
        .dropna()\
        .dropDuplicates()\
        .persist()\
        .join(df_population, 'COUNTRY_SHORT_NAME', 'right_outer')\
        .selectExpr(
            'CONTINENT_NAME as continent',
            'COUNTRY_SHORT_NAME as country',
            'PROVINCE_STATE_NAME as province',
            'DOUBLE(GEO_LATITUDE) as geo_latitude',
            'DOUBLE(GEO_LONGITUDE) as geo_longitude') \
        .dropDuplicates(subset=['continent', 'country', 'province'])

    df_conutries.write.parquet(
        os.path.join(output_loc, 'dim_countries'), mode='overwrite')

    return df_conutries

def _run_fact_activity(
        df_activity, df_population,
        df_conutries, 
        df_vacc_global,
        df_vacc_usa_doses,
        df_vacc_usa_people,
        output_loc):

    df_population_clean = df_population.selectExpr(
        'COUNTY_NAME',
        'PROVINCE_STATE_NAME',
        'COUNTRY_ALPHA_3_CODE',
        'COUNTRY_SHORT_NAME',
        'COUNTRY_ALPHA_2_CODE',
        'DOUBLE(GEO_LATITUDE) as GEO_LATITUDE',
        'DOUBLE(GEO_LONGITUDE) as GEO_LONGITUDE',
        'INT(GEO_REGION_POPULATION_COUNT) as GEO_REGION_POPULATION_COUNT'
    ).fillna(0)


    df_poplucation_clean_by_province = df_population_clean.groupBy(
            'COUNTRY_SHORT_NAME', 
            'PROVINCE_STATE_NAME',
            'COUNTRY_ALPHA_3_CODE',
            'COUNTRY_ALPHA_2_CODE').agg(
                F.sum('GEO_REGION_POPULATION_COUNT').alias('GEO_REGION_POPULATION_COUNT'),
                F.avg('GEO_LATITUDE').alias('GEO_LATITUDE'),
                F.avg('GEO_LONGITUDE').alias('GEO_LONGITUDE'),
            )
            

    # get all doses administered data

    df_cov19_fact_vacc_doses_1 = df_vacc_global.selectExpr(
        'Country_Region as country',
        'Province_State as province',
        'Date as date',
        'INT(Doses_admin) as doses_admin'
    ).withColumn('vacc_type', F.lit('Unknown'))

    df_cov19_fact_vacc_doses_2 = df_vacc_usa_doses.filter(valid_vaccination_type_condition)\
        .selectExpr(
            'Country_Region as country',
            'Province_State as province',
            'Date as date',
            'INT(Doses_admin) as doses_admin',
            'Vaccine_Type as vacc_type'
    ).dropna(subset=['doses_admin'])

    df_cov19_fact_vacc_doses_union = df_cov19_fact_vacc_doses_1.union(df_cov19_fact_vacc_doses_2)\
        .groupBy('country', 'province', 'date').sum('doses_admin').\
        select('country', 'province', 'date', F.col('sum(doses_admin)').alias('doses_admin'))

    df_cov19_fact_vacc_doses = df_cov19_fact_vacc_doses_union\
        .join(df_poplucation_clean_by_province, (
            (
                (df_cov19_fact_vacc_doses_union.country == df_poplucation_clean_by_province.COUNTRY_SHORT_NAME) |
                (df_cov19_fact_vacc_doses_union.country == df_poplucation_clean_by_province.COUNTRY_ALPHA_3_CODE) |
                (df_cov19_fact_vacc_doses_union.country == df_poplucation_clean_by_province.COUNTRY_ALPHA_2_CODE) 
            ) & (
                (df_cov19_fact_vacc_doses_union.province == df_poplucation_clean_by_province.PROVINCE_STATE_NAME) |
                (df_cov19_fact_vacc_doses_union.province.isNull() & 
                df_poplucation_clean_by_province.PROVINCE_STATE_NAME.isNull())
            )
        ), 'left_outer')

    # get all vaccinated people data

    df_cov19_fact_vacc_people_1 = df_vacc_global.selectExpr(
        'Country_Region as country',
        'Province_State as province',
        'Date as date',
        'INT(People_partially_vaccinated) as people_partially_vaccinated',
        'INT(People_fully_vaccinated) as people_fully_vaccinated',
    )

    df_cov19_fact_vacc_people_2 = df_vacc_usa_people.selectExpr(
        'Country_Region as country',
        'Province_State as province',
        'Date as date',
        'INT(People_partially_vaccinated) as people_partially_vaccinated',
        'INT(People_fully_vaccinated) as people_fully_vaccinated',
    )

    df_cov19_fact_vacc_people_union = df_cov19_fact_vacc_people_1.union(df_cov19_fact_vacc_people_2)
    df_cov19_fact_vacc_people = df_cov19_fact_vacc_people_union\
        .join(df_poplucation_clean_by_province, (
            (
                (df_cov19_fact_vacc_people_union.country == df_poplucation_clean_by_province.COUNTRY_SHORT_NAME) |
                (df_cov19_fact_vacc_people_union.country == df_poplucation_clean_by_province.COUNTRY_ALPHA_3_CODE) |
                (df_cov19_fact_vacc_people_union.country == df_poplucation_clean_by_province.COUNTRY_ALPHA_2_CODE) 
            ) & (
                (df_cov19_fact_vacc_people_union.province == df_poplucation_clean_by_province.PROVINCE_STATE_NAME) |
                (df_cov19_fact_vacc_people_union.province.isNull() & 
                df_poplucation_clean_by_province.PROVINCE_STATE_NAME.isNull())
            )
        ), 'left_outer')

    df_activity_clean = df_activity.selectExpr(
        'INT(PEOPLE_POSITIVE_CASES_COUNT) as PEOPLE_POSITIVE_CASES_COUNT',
        'COUNTY_NAME',
        'PROVINCE_STATE_NAME',
        'REPORT_DATE',
        'CONTINENT_NAME',
        'DATA_SOURCE_NAME',
        'INT(PEOPLE_DEATH_NEW_COUNT) as PEOPLE_DEATH_NEW_COUNT',
        'COUNTRY_ALPHA_3_CODE',
        'COUNTRY_SHORT_NAME',
        'COUNTRY_ALPHA_2_CODE',
        'INT(PEOPLE_POSITIVE_NEW_CASES_COUNT) as PEOPLE_POSITIVE_NEW_CASES_COUNT',
        'INT(PEOPLE_DEATH_COUNT) as PEOPLE_DEATH_COUNT'
    ).fillna(0)


    df_cov19_fact_activity = df_activity_clean\
        .groupBy(
            'REPORT_DATE',
            'CONTINENT_NAME',
            'COUNTRY_ALPHA_3_CODE',
            'COUNTRY_ALPHA_2_CODE',
            'DATA_SOURCE_NAME',
            df_activity_clean['COUNTRY_SHORT_NAME'], 
            df_activity_clean['PROVINCE_STATE_NAME']).agg(
                F.sum('PEOPLE_POSITIVE_CASES_COUNT').alias('PEOPLE_POSITIVE_CASES_COUNT'),
                F.sum('PEOPLE_DEATH_NEW_COUNT').alias('PEOPLE_DEATH_NEW_COUNT'),
                F.sum('PEOPLE_POSITIVE_NEW_CASES_COUNT').alias('PEOPLE_POSITIVE_NEW_CASES_COUNT'),
                F.sum('PEOPLE_DEATH_COUNT').alias('PEOPLE_DEATH_COUNT'))\
        .join(df_poplucation_clean_by_province, (
            (df_activity_clean.COUNTRY_SHORT_NAME == df_poplucation_clean_by_province.COUNTRY_SHORT_NAME) &
            (
                (df_activity_clean.PROVINCE_STATE_NAME == df_poplucation_clean_by_province.PROVINCE_STATE_NAME) | 
                (df_activity_clean.PROVINCE_STATE_NAME.isNull() | 
                    df_poplucation_clean_by_province.PROVINCE_STATE_NAME.isNull()))
        ), 'left_outer')\
        .join(df_cov19_fact_vacc_doses, (
                (df_cov19_fact_vacc_doses.country == df_activity_clean.COUNTRY_SHORT_NAME) |
                (df_cov19_fact_vacc_doses.country == df_activity_clean.COUNTRY_ALPHA_3_CODE) |
                (df_cov19_fact_vacc_doses.country == df_activity_clean.COUNTRY_ALPHA_2_CODE) 
            ) & (
                (df_cov19_fact_vacc_doses.province == df_activity_clean.PROVINCE_STATE_NAME) | 
                (df_cov19_fact_vacc_doses.province.isNull() & df_activity_clean.PROVINCE_STATE_NAME.isNull())
            ) & (
                df_cov19_fact_vacc_doses.date == df_activity_clean.REPORT_DATE
            ), 'outer')\
        .join(df_cov19_fact_vacc_people, (
                (df_cov19_fact_vacc_people.country == df_activity_clean.COUNTRY_SHORT_NAME) |
                (df_cov19_fact_vacc_people.country == df_activity_clean.COUNTRY_ALPHA_3_CODE) |
                (df_cov19_fact_vacc_people.country == df_activity_clean.COUNTRY_ALPHA_2_CODE) 
            ) & (
                (df_cov19_fact_vacc_people.province == df_activity_clean.PROVINCE_STATE_NAME) |
                (df_cov19_fact_vacc_people.province.isNull() & df_activity_clean.PROVINCE_STATE_NAME.isNull())
            ) & (
                df_cov19_fact_vacc_people.date == df_activity_clean.REPORT_DATE
            ), 'outer'
        )


        # pick up first valid value from columns
    mergeF = F.udf(lambda *args: next(
                arg for arg in args if bool(arg)
            ) if any(args) else None
    )

    df_cov19_fact_activity_renames = df_cov19_fact_activity.select(
        mergeF(
            df_activity_clean.REPORT_DATE, 
            df_cov19_fact_vacc_people.date, 
            df_cov19_fact_vacc_doses.date).alias('date'),
        mergeF(
            df_activity_clean.COUNTRY_SHORT_NAME,
            df_cov19_fact_vacc_people.country,
            df_cov19_fact_vacc_doses.country,
            df_cov19_fact_vacc_people.COUNTRY_SHORT_NAME,
            df_cov19_fact_vacc_doses.COUNTRY_SHORT_NAME
            ).alias('country'),
        mergeF(
            df_activity_clean.PROVINCE_STATE_NAME,
            df_cov19_fact_vacc_people.province,
            df_cov19_fact_vacc_people.PROVINCE_STATE_NAME,
            df_cov19_fact_vacc_doses.province,
            df_cov19_fact_vacc_doses.PROVINCE_STATE_NAME
        ).alias('province'),
        F.col('PEOPLE_POSITIVE_CASES_COUNT').alias('people_positives_cases_count'),
        F.col('PEOPLE_DEATH_NEW_COUNT').alias('people_death_new_count'),
        F.col('PEOPLE_POSITIVE_NEW_CASES_COUNT').alias('people_positive_new_cases_count'),
        F.col('PEOPLE_DEATH_COUNT').alias('people_death_count'),
        F.col('DATA_SOURCE_NAME').alias('data_source_name'),
        mergeF(
            df_poplucation_clean_by_province.GEO_REGION_POPULATION_COUNT,
            df_cov19_fact_vacc_people.GEO_REGION_POPULATION_COUNT,
            df_cov19_fact_vacc_doses.GEO_REGION_POPULATION_COUNT,
        ).alias('population'),
        df_cov19_fact_vacc_people.people_partially_vaccinated,
        df_cov19_fact_vacc_people.people_fully_vaccinated,
        df_cov19_fact_vacc_doses.doses_admin,
    ).join(
            df_conutries,
            'country',
            'left_outer'
        ).drop(df_conutries.province)\
        .dropDuplicates(['date', 'continent', 'country', 'province']).fillna(0)

    df_cov19_fact_activity_renames.write.parquet(
            os.path.join(output_loc, 'fact_activity'), mode='overwrite')

def run(input_loc, output_loc):
    """
    This is a dummy function to show how to use spark, It is supposed to mock
    the following steps
        1. clean input data
        2. use a pre-trained model to make prediction 
        3. write predictions to a HDFS output

    Since this is meant as an example, we are going to skip building a model,
    instead we are naively going to mark reviews having the text "good" as positive and
    the rest as negative 
    """

    df_activity, df_population, df_vacc_global, df_vacc_usa_doses, df_vacc_usa_people = read_sources(spark, input_loc)

    # calculate dimentions
    _run_dim_continent(df_activity, df_population)
    _run_dim_data_sources(df_activity, df_population, output_loc)
    _run_dim_vaccinations_types(df_vacc_usa_doses, output_loc)
    df_countries = _run_dim_countries(df_activity, df_population, output_loc)

    # calculate fact table
    _run_fact_activity(
        df_activity, df_population,
        df_countries, 
        df_vacc_global,
        df_vacc_usa_doses,
        df_vacc_usa_people,
        output_loc)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, help="HDFS input", default="/data")
    parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    run(input_loc=args.input, output_loc=args.output)
