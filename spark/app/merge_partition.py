#
# Merge Oucomes and Places data. 
# Removed duplicates, rename columns according the Spec
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

outcomes_path = sys.argv[1]
places_path = sys.argv[2]
merged_data_path = sys.argv[3]

def partition_data(df: pysql.DataFrame, to_path: str):
    '''
        Partition data by districtName.
        In this case repartitioning is not so effective and doesn't make sense becase we have 
        small piece of data. It is used for educational purpose. But for big data, such 
        repartitioning could help to fetch filter data much quickly bu Spark. 
    '''
    df.repartition(2, "districtName").write.partitionBy("districtName").parquet(to_path, mode='overwrite')


def main():
    '''
        Merge Oucomes and Places data. 
        Removed duplicates, rename columns according the Spec
    '''
    outcome_df = (spark.read
                .option("header", "true")
                .parquet(outcomes_path)
                .select('Crime ID', 'Outcome type')
    )
    logging.info(f"Outcomes count: {outcome_df.count()}")

    places_df = (spark.read
                    .option("header", "true")
                    .parquet(places_path)
                    .select('Crime ID', 'Month', 'Reported by', 'Longitude', 'Latitude', 'Crime type', 'Last outcome category')
    )
    logging.info(f"Places count: {places_df.count()}")
    
    logging.info("Merging Places and Outcomes")
    df_all = places_df.join(outcome_df, on='Crime ID', how='left')
    
    logging.info("###### Data Dedupliction #######")
    logging.info(f"Before deduplication, count: {df_all.count()}")
    df_all = df_all.dropDuplicates()
    logging.info(f"Rows remained after deduplication, count: {df_all.count()}")

    # rename columns, add lastOutcome 
    df_all = (df_all.withColumn('lastOutcome', 
                F.when(F.col('Outcome type') != 'null', F.col('Outcome type'))
                .otherwise(F.col('Last outcome category')))            
            .withColumnRenamed('Crime ID', 'crimeId')
            .withColumnRenamed('Reported by', 'districtName')
            .withColumnRenamed('Longitude', 'longitude')
            .withColumnRenamed('Latitude', 'latitude')
            .withColumnRenamed('Crime type', 'crimeType')
            .select('crimeId', 'districtName', 'latitude', 'longitude', 'crimeType', 'lastOutcome'))

    partition_data(df_all, merged_data_path)

if __name__ == "__main__":
    main()