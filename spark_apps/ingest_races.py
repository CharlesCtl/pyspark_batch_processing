import sys
from pyspark.sql import *
from pyspark.sql.functions import col,current_timestamp,to_timestamp,concat,lit
from lib.logger import Log4j
from lib.utils import get_spark_app_config,load_races_df


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

    races_df = load_races_df(spark,sys.argv[1]) 
    races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

    races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuit", "circuit_id")

    races_final_df = races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(races_renamed_df.date,lit(' '),races_renamed_df.time),"yyyy-MM-dd HH:mm:ss")) \
        .withColumn("ingestion_date", current_timestamp())
    races_final_df.write.mode("overwrite").parquet("/opt/spark/data/processed/races")
    
    #---------------------------------
    logger.info("Finished Hello Spark")
    spark.stop()