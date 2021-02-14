import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),    
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.crime.sanfrancisco.v1") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("spark.streaming.ui.retainedBatches", "100") \
        .option("spark.streaming.ui.retainedStages", "100") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value as STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table \
        .select('original_crime_type_name', 
                'disposition', 
                psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time")
        ).distinct()

    distinct_table.printSchema()
    
    # count the number of original crime type
    agg_df = distinct_table\
        .select("original_crime_type_name") \
        .groupby("original_crime_type_name") \
        .count()

    query = agg_df \
            .writeStream \
            .format("console") \
            .outputMode("complete") \
            .start()

    query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    join_query = agg_df.join(radio_code_df, "disposition") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    join_query.awaitTermination()

    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .getOrCreate()
    
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
