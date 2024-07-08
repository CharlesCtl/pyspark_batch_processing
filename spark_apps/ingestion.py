import sys
sys.path.append('/opt/spark/apps/lib')
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_spark_app_config

from ingestion_apps.ingest_circuits import ingest_and_transform_circuits
from ingestion_apps.ingest_constructors import ingest_and_transform_constructors
from ingestion_apps.ingest_drivers import ingest_and_transform_drivers
from ingestion_apps.ingest_lap_times import ingest_and_transform_lap_times
from ingestion_apps.ingest_pitstops import ingest_and_transform_pitstops
from ingestion_apps.ingest_qualifying import ingest_and_transform_qualifyng
from ingestion_apps.ingest_races import ingest_and_transform_races
from ingestion_apps.ingest_results import ingest_and_transform_results
circuits_data_file = '/opt/spark/data/raw_data/circuits.csv'
constructors_data_file = '/opt/spark/data/raw_data/constructors.json'
drivers_data_file = '/opt/spark/data/raw_data/drivers.json'
lap_time_data_file = '/opt/spark/data/raw_data/lap_times'
pitstops_data_file = '/opt/spark/data/raw_data/pit_stops.json'
qualifying_data_file = '/opt/spark/data/raw_data/qualifying'
races_data_file = '/opt/spark/data/raw_data/races.csv'
results_data_file = '/opt/spark/data/raw_data/results.json'
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
    ingest_and_transform_circuits(spark,circuits_data_file)
    ingest_and_transform_constructors(spark,constructors_data_file)
    ingest_and_transform_drivers(spark,drivers_data_file)
    ingest_and_transform_lap_times(spark,lap_time_data_file)
    ingest_and_transform_pitstops(spark,pitstops_data_file)
    ingest_and_transform_qualifyng(spark,qualifying_data_file)
    ingest_and_transform_races(spark,races_data_file)
    ingest_and_transform_results(spark,results_data_file)
    #---------------------------------
    logger.info(f"========================   Finished App {sys.argv[0].split('/')[4]}    ========================")
    spark.stop()