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

    if len(sys.argv) != 2:
        logger.error("Usage: App <filename>")
        sys.exit(-1)

    logger.info(f"---------------- !!!Starting {sys.argv[0]}!!! ----------------")
    
    parquetFile = spark.read.parquet(f"/opt/spark/data/processed/{sys.argv[1]}")
    parquetFile.show()
    #---------------------------------
    logger.info(f"---------------- !!!Finished {sys.argv[0]}!!! ----------------")
    spark.stop()    