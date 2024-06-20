import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

from ingestion_apps.ingest_circuits import ingest_circuits

circuits_data_file = '/opt/spark/data/circuits.csv'
if __name__=="__main__":
    conf = get_spark_app_config()
    
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4j(spark)
    """
    if len(sys.argv) != 2:
        logger.error("Usage: App <filename>")
        sys.exit(-1)
    """
    logger.info(f"========================   Starting App {sys.argv[0].split('/')[4]}    ========================")
    ingest_circuits(spark,circuits_data_file)
    #---------------------------------
    logger.info(f"========================   Finished App {sys.argv[0].split('/')[4]}    ========================")
    spark.stop()