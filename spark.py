import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DoubleType
 
 
# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")
 
 
def initialize_spark_session(app_name, access_key, secret_key):
    """
    Initialize the Spark Session with provided configurations.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for S3.
    :param secret_key: Secret key for S3.
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession \
                .builder \
                .appName(app_name) \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                .getOrCreate()
 
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully')
        return spark
 
    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None
 
 
def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
 
    try:
        logger.info("Trying to get the data")
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        return df
 
    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None
 
 
def transform_streaming_data(df):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    # Define the schema for the 'coordonnees_geo' field (nested object)
    coordinates_schema = StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ])
    # Define the overall schema, including 'total_count' and 'results'
    schema = StructType([
        StructField("total_count", IntegerType(), True),
        StructField("results", ArrayType(StructType([
            StructField("stationcode", StringType(), True),
            StructField("name", StringType(), True),
            StructField("is_installed", StringType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("numdocksavailable", IntegerType(), True),
            StructField("numbikesavailable", IntegerType(), True),
            StructField("mechanical", IntegerType(), True),
            StructField("ebike", IntegerType(), True),
            StructField("is_renting", StringType(), True),
            StructField("is_returning", StringType(), True),
            StructField("duedate", StringType(), True),
            StructField("coordonnees_geo", coordinates_schema, True),
            StructField("nom_arrondissement_communes", StringType(), True),
            StructField("code_insee_commune", StringType(), True)
        ]), True))
    ])
    
    #retreive the raw json data from kafka
    raw_df=df.select(from_json(col('value').cast('string'),schema).alias('data'),col('offset'),col('timestamp')).select(col('timestamp'),col('offset'),col('data.*'))
    #transform the json file into columnar dataset
    #Explode the results array:
    transformed_df=raw_df.select(col('timestamp'),col('offset'),explode(col("results")).alias('results'))\
                         .select(col('timestamp'),col('offset'),col('results.*'),col("results.coordonnees_geo.lon").alias("longitude"),col("results.coordonnees_geo.lat").alias("latitude")).drop('coordonnees_geo')
    return transformed_df
 
 
def initiate_streaming_to_bucket(df, path, checkpoint_location):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.
    
    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :param checkpoint_location: Checkpoint location for streaming.
    :return: None
    """
 
    logger.info("Initiating streaming process...")
    stream_query = df.writeStream \
                    .outputMode("append") \
                    .option("path", path) \
                    .option("checkpointLocation", checkpoint_location) \
                    .start()
    stream_query.awaitTermination()
 

def main():
    app_name = "kafka-streaming"
    access_key = "access-key"
    secret_key = "secret-key"
    brokers = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
    topic = "velib_test"
    path = "s3a://kafka-streaming-bucket-rg/velib_data/"
    checkpoint_location = "s3a://kafka-streaming-bucket-rg/checkpoint/"
    

    spark = initialize_spark_session(app_name, access_key, secret_key)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()





