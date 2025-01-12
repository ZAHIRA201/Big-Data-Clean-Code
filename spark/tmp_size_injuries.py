import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, udf, col
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    StructField,
    DoubleType,
    BooleanType,
)

spark = SparkSession.builder.appName("tmp_size_injuries").getOrCreate()
tmp_mix = (
    spark.read.option("delimiter", ",")
    .option("escapeQuotes", "true")
    .csv(
        "/spark/1/tmp_mix",
        schema=StructType(
            [
                StructField("company_name", StringType()),
                StructField("industry", StringType()),
                StructField("size_range", StringType()),
                StructField("city", StringType()),
                StructField("total_deaths", IntegerType()),
                StructField("total_injuries", IntegerType()),
                StructField("total_employee_estimate", IntegerType()),
            ]
        ),
    )
)
out_size_injuries = tmp_mix.groupBy("size_range").agg(
    avg("total_injuries").alias("avg_total_injuries"),
    avg("total_deaths").alias("avg_total_deaths"),
    avg("total_employee_estimate").alias("avg_total_employee_estimate"),
)
out_size_injuries.write.mode("overwrite").option("delimiter", ",").csv(
    "/spark/1/out_size_injuries"
)
spark.stop()
