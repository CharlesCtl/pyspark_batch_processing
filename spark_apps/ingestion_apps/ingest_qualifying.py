import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_qualifying_df,transform_qualifying_df
def ingest_and_transform_qualifyng(spark,file_path):
    qualifying_df = load_qualifying_df(spark,file_path)
    qualifying_final_df = transform_qualifying_df(qualifying_df)
    qualifying_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/qualifying")
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
    ingest_and_transform_qualifyng(spark,sys.argv[1])
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()