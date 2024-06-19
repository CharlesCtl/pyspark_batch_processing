import sys
from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_races_df,transform_races_df


if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: App <filename>")
        sys.exit(-1)
    
    logger.info("¡¡¡ STARTING APP  !!!")

    races_df = load_races_df(spark,sys.argv[1])
    races_final_df = transform_races_df(races_df)
    """
    logger.info("Num partitions before: "+ str(races_final_df.rdd.getNumPartitions()))
    races_final_df.groupby(spark_partition_id()).count().show()

    partitioned_df = races_final_df.repartition(3)
    logger.info("Num partitions after: "+ str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupby(spark_partition_id()).count().show()
    """
    races_final_df.write.mode("overwrite").partitionBy('race_year')\
    .parquet("/opt/spark/data/processed/races")
    
    #---------------------------------
    logger.info("¡¡¡ FINISHING APP !!!")
    spark.stop()