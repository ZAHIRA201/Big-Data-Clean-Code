import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, udf
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    StructField,
    DoubleType,
)

spark = SparkSession.builder.appName("tmp_injuries_filtered").getOrCreate()
schema = StructType(
    [
        StructField("company_name", StringType()),
        StructField("establishment_name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("no_injuries_illnesses", DoubleType()),
        StructField("total_deaths", IntegerType()),
        StructField("total_dafw_cases", IntegerType()),
        StructField("total_djtr_cases", IntegerType()),
        StructField("total_other_cases", IntegerType()),
        StructField("total_injuries", IntegerType()),
        StructField("total_poisonings", IntegerType()),
        StructField("total_respiratory_conditions", IntegerType()),
        StructField("total_skin_disorders", IntegerType()),
        StructField("total_hearing_loss", IntegerType()),
    ]
)
injuries = (
    spark.read.option("delimiter", ",")
    .option("escapeQuotes", "true")
    .csv("/user/root/companies/INJURIES.csv", schema=schema)
)
injuries = injuries.sample(0.1)
tmp_injuries_filtered = injuries.filter(col("no_injuries_illnesses") == 1.0)
tmp_injuries_filtered = tmp_injuries_filtered.select(
    col("company_name"),
    col("establishment_name"),
    col("city"),
    col("total_deaths"),
    col("total_injuries"),
)
tmp_injuries_filtered.write.mode("overwrite").option("delimiter", ",").csv(
    "/spark/1/tmp_injuries_filtered"
)
spark.stop()
