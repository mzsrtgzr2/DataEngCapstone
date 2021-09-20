"""
Helper methods.
"""
import logging
from itertools import chain
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.functions import create_map, lit

from utils.io import provide_config, read_with_meta


def uppercase_columns(df: DataFrame, col_list: List) -> DataFrame:
    """
    Rewrite the selected columns with upper cases
    :param df: dataframe
    :param col_list: string array of columns to be upper-cased
    :return: dataframe
    """
    for col in col_list:
        df = df.withColumn(col, F.upper(F.col(col)))
        df = df.withColumn(col, F.regexp_replace(F.col(col), 'İ', 'I'))
        df = df.withColumn(col, F.trim(F.col(col)))
    logging.info(f"{col_list} columns are converted to uppercase")
    return df


def melt(df: DataFrame, key_cols: Iterable[str], value_cols: Iterable[str],
         var_name: str = "variable", value_name: str = "value") -> DataFrame:
    """
    Convert wide dataframe format into long format
    :param df: Wide dataframe
    :param key_cols: Key columns to be remained after conversion.
    :param value_cols: Value columns to be converted into variable and value columns.
    :param var_name: Column name of variable.
    :param value_name: Column name of value.
    :return: Long dataframe
    """
    # Create map<key: value>
    vars_and_vals = F.create_map(
        list(chain.from_iterable([
            [F.lit(c), F.col(c)] for c in value_cols]
        ))
    )

    df = (
        df
            .select(*key_cols, F.explode(vars_and_vals))
            .withColumnRenamed('key', var_name)
            .withColumnRenamed('value', value_name)
            .filter(F.col(value_name).isNotNull())
    )
    logging.info("Wide dataframe converted to long dataframe")
    return df


def get_country_id(spark, df: DataFrame, config: Dict) -> DataFrame:
    """
    Get country id from its dimension table
    :param spark: Spark session
    :param df: dataframe
    :param config: string array of columns to be upper-cased
    :return: dataframe
    """
    country = read_with_meta(spark, config['country_meta'])
    key_col = 'country_name'
    df = df.join(country, on=key_col, how='inner')
    df = df.drop(key_col)
    logging.info("Country name is converted to id from dimension table")
    return df


def add_decade_column(df: DataFrame, date_col: str = 'date') -> DataFrame:
    """
    Add year and decade columns from date column.
    :param df: dataframe including date column
    :param date_col: column name of date
    :return: dataframe
    """
    df = df.withColumn('year', F.year(date_col))
    df = df.withColumn('decade', (F.floor(F.col('year') / 10) * 10).cast('string'))
    df = df.withColumn('decade', F.concat('decade', F.lit('s')))
    logging.info("Decade and year columns are generated from date column")
    return df


def add_rank_column(df: DataFrame, partition_col: str, order_by_col: str, rank_col: str,
                    ascending: bool = False) -> DataFrame:
    """
    Add rank column by partitions

    :param df: Dataframe to add rank column
    :param partition_col: Partition columns for ranking
    :param order_by_col: Sort column for ranking
    :param rank_col: Name of rank column to be added
    :param ascending: Indicates the sorting is ascending or descending
    :return:
    """
    if not ascending:
        w = Window.partitionBy(partition_col).orderBy(F.col(order_by_col).desc())
    else:
        w = Window.partitionBy(partition_col).orderBy(order_by_col)
    df = df.withColumn(rank_col, F.row_number().over(w))
    logging.info(f"{rank_col} calculated by {partition_col}")
    return df


def correct_country_names(df: DataFrame, country_col: str, country_mapping_path: str,
                                ) -> DataFrame:
    """
    Replace corrupted country values with true ones.

    :param df: dataframe including country_name column
    :param country_col: Column name of country
    :param country_mapping_path: Path of mapping config
    :return: dataframe including country_name columns
    """
    column = country_col
    replace_dict = provide_config(country_mapping_path)
    corrupted_values = list(replace_dict.keys())
    map_col = create_map([lit(x) for x in chain(*replace_dict.items())])
    df = df.withColumn(column, F.regexp_replace(column, '"', ''))
    df = df.withColumn(column, F.when(F.col(column).isin(corrupted_values), map_col[df[column]]
                                      ).otherwise(F.col(column)))
    df = df.filter(F.col(column).isNotNull())
    df = df.drop_duplicates()
    logging.info("Corrupted country columns are replaced with true values")
    return df
