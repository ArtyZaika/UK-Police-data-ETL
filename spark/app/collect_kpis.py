#
# Collects statistics according the data
#
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql as pysql
import logging

spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

merged_data_path = sys.argv[1]
statistics_path = sys.argv[2]

def collect_statistics(df_all: pysql.DataFrame):
    """
    Takes previously merged Outcome and Places dataset and calculates statistics

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """

    logging.info(f"Aggregating Crime type stats...")
    check_df = agg_crime_types_count(df_all)
    assert 100.0 == round(check_df.select(F.sum("percent").alias('prc')).collect()[0]['prc'], 0)
    check_df.coalesce(1).write.parquet(f'{statistics_path}/crime_types_count/', mode='overwrite')

    logging.info(f"Aggregating Crime type by Distric stats...")
    check_df = agg_crime_types_by_disctrict_count(df_all)
    assert 100.0 == round(check_df.select(F.sum("percent").alias('prc')).collect()[0]['prc'], 0)
    check_df.coalesce(1).write.parquet(f'{statistics_path}/crime_types_by_disctrict_count/', mode='overwrite')

    logging.info(f"Aggregating Outcomes by Distric stats...")
    check_df = agg_outcome_by_disctrict_count(df_all)
    assert 100.0 == round(check_df.select(F.sum("percent").alias('prc')).collect()[0]['prc'], 0)
    check_df.coalesce(1).write.parquet(f'{statistics_path}/outcome_by_disctrict_count/', mode='overwrite')

    logging.info(f"Aggregating Occurences by Distric stats...")
    check_df = agg_occurences_by_district(df_all)
    assert 100.0 == round(check_df.select(F.sum("percent").alias('prc')).collect()[0]['prc'], 0)
    check_df.coalesce(1).write.parquet(f'{statistics_path}/occurences_by_district/', mode='overwrite')

    logging.info(f"Aggregating Outcomes stats...")
    check_df = agg_outcomes_count(df_all)
    assert 100.0 == round(check_df.select(F.sum("percent").alias('prc')).collect()[0]['prc'], 0)
    check_df.coalesce(1).write.parquet(f'{statistics_path}/outcomes_count/', mode='overwrite')

def agg_crime_types_count(df: pysql.DataFrame):
    """
    Calculates Crime Type %

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """
    total = df.count()
    return (df.groupBy('crimeType').agg(F.count('crimeType').alias('cases'))
             .withColumn('percent', F.round(100.0 * F.col('cases')/total, 2))
             .orderBy(F.desc('percent'))
             )

def agg_crime_types_by_disctrict_count(df: pysql.DataFrame):
    """
    Calculates Crime Type % by District type

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """
    _df = df.groupBy('crimeType', 'districtName').agg(F.count('crimeType').alias('cases'))
    total = df.count()

    return (_df.withColumn('percent', F.round(100.0 * F.col('cases')/total, 2))
               .orderBy(F.desc('percent'))
             )

def agg_outcome_by_disctrict_count(df: pysql.DataFrame):
    """
    Calculates Outcome % by District type

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """
    _df = df.groupBy('lastOutcome', 'districtName').agg(F.count('lastOutcome').alias('cases'))
    total = df.count()

    return (_df.withColumn('percent', F.round(100.0 * F.col('cases')/total, 2))
               .orderBy(F.desc('percent'))
             )

def agg_occurences_by_district(df: pysql.DataFrame):
    """
    Calculates Occurences % by District type

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """
    _df = df.groupBy('districtName').agg(F.count('crimeType').alias('cases'))
    total = df.count()

    return (_df.withColumn('percent', F.round(100.0 * F.col('cases')/total, 2))
               .orderBy(F.desc('percent'))
             )

def agg_outcomes_count(df: pysql.DataFrame):
    """
    Calculates Outcomes % 

    Parameters
    ----------
    df_all : Spark DataFrame of merged Outcome and Places data
    """
    total = df.count()
    return (df.groupBy('lastOutcome').agg(F.count('lastOutcome').alias('cases'))
             .withColumn('percent', F.round(100.0 * F.col('cases')/total, 2))
             .orderBy(F.desc('percent'))
             )

def main():
    df_all = (spark.read
                .option("header", "true")
                .parquet(merged_data_path)
    )    
    collect_statistics(df_all)


if __name__ == "__main__":
    main()