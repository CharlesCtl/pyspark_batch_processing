import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_drivers_df,transform_drivers_df

def ingest_and_transform_drivers(spark,file_path):
    drivers_df = load_drivers_df(spark,file_path)   
    drivers_final_df = transform_drivers_df(drivers_df)
    drivers_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/drivers")
    return True

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: App <filename>")
        sys.exit(-1)
    
    logger.info("Starting App!!!")
    ingest_and_transform_drivers(spark,sys.argv[1])
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()