import configparser
from pyspark import SparkConf
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType
from pyspark.sql.functions import col,current_timestamp,concat,lit

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
def load_races_df(spark, data_file):
    return spark.read\
        .option("header","true") \
        .schema(races_schema) \
        .csv(data_file)
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