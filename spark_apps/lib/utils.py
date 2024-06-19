import configparser
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType,FloatType
from pyspark.sql.functions import col,current_timestamp,concat,lit
from pyspark.sql.functions import col,current_timestamp,to_timestamp,concat,lit,spark_partition_id
def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("/opt/spark/apps/spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])
constructor_schema = constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
])
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
                                    ])
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])
def load_circuits_df(spark, data_file):
    return spark.read\
        .option("header","true") \
        .schema(circuits_schema) \
        .csv(data_file)
def transform_circuits_df(circuits_df):
    circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
    circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") 
    return circuits_renamed_df.withColumn("ingestion_date", current_timestamp())
#---------------------------------------
def load_races_df(spark, data_file):
    return spark.read\
            .option("header","true") \
            .schema(races_schema) \
            .csv(data_file)
def transform_races_df(races_df):
    return races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time")) \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuit", "circuit_id") \
    .withColumn("race_timestamp",to_timestamp(concat(races_df.date,lit(' '),races_df.time),"yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ingestion_date", current_timestamp())
#---------------------------------------
def load_constructors_df(spark, data_file):
    return spark.read\
            .option("header","true") \
            .schema(constructor_schema) \
            .json(data_file)
def transform_constructors_df(constructor_df):
    return constructor_df.drop("url")\
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumnRenamed("constructorRef", "constructor_ref") \
            .withColumn("ingestion_date", current_timestamp())
#---------------------------------------
def load_drivers_df(spark, data_file):
    return spark.read\
            .option("header","true") \
            .schema(drivers_schema) \
            .json(data_file)
def transform_drivers_df(drivers_df):
    return drivers_df.withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("driverRef", "driver_ref") \
            .withColumn("ingestion_date", current_timestamp()) \
            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
            .drop(col("url"))
#---------------------------------------
def load_results_df(spark, data_file):
    return spark.read\
            .schema(results_schema) \
            .json(data_file)
def transform_results_df(results_df):
    return results_df.withColumnRenamed("resultId", "result_id") \
            .withColumnRenamed("raceId", "race_id") \
            .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumnRenamed("positionText", "position_text") \
            .withColumnRenamed("positionOrder", "position_order") \
            .withColumnRenamed("fastestLap", "fastest_lap") \
            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
            .withColumn("ingestion_date", current_timestamp()) \
            .drop(col("statusId"))
#---------------------------------------
def load_pitstops_df(spark,data_file):
    return spark.read\
            .schema(pit_stops_schema) \
            .option("multiline",True) \
            .json(data_file)
def transform_pitstops_df(pitstops_df):
    return pitstops_df.withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id") \
            .withColumn("ingestion_date", current_timestamp())
#---------------------------------------
def load_lap_times_df(spark, data_dir):
    return spark.read\
            .schema(lap_times_schema) \
            .csv(data_dir)
def transform_lap_times_df(lap_times_df):
    return lap_times_df.withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id") \
            .withColumn("ingestion_date", current_timestamp())
#---------------------------------------
def load_qualifying_df(spark,data_dir):
    return spark.read\
            .schema(qualifying_schema) \
            .option("multiline",True) \
            .json(data_dir)
def transform_qualifying_df(qualifying_df):
    return qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
            .withColumnRenamed("driverId", "driver_id") \
            .withColumnRenamed("raceId", "race_id") \
            .withColumnRenamed("constructorId", "constructor_id") \
            .withColumn("ingestion_date", current_timestamp())