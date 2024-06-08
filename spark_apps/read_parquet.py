import sys
from pyspark.sql import *
from lib.utils import get_spark_app_config
from lib.logger import Log4j
if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4j(spark)
    logger.info("Starting App!!!")
    
    parquetFile = spark.read.parquet("/opt/spark/data/processed/circuits")
    parquetFile.show()
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()    