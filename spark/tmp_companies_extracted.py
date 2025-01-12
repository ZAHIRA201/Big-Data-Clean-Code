import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, udf
from pyspark.sql.types import StructType, StringType, IntegerType, StructField


def extract_city_from_locality(locality: str) -> str:
    if not locality:
        return None
    return locality.split(", ")[0]


extractUDF = udf(lambda z: extract_city_from_locality(z), StringType())
spark = SparkSession.builder.appName("tmp_companies_extracted").getOrCreate()
schema = StructType(
    [
        StructField("name", StringType()),
        StructField("industry", StringType()),
        StructField("size_range", StringType()),
        StructField("locality", StringType()),
        StructField("total_employee_estimate", IntegerType()),
    ]
)
companies = (
    spark.read.option("delimiter", ",")
    .option("escapeQuotes", "true")
    .csv("/user/root/companies/COMPANIES.csv", schema=schema)
)
companies = companies.sample(0.1)
tmp_companies_extracted = companies.select(
    col("name"),
    when(col("industry") == "", "Empty").otherwise(col("industry")).alias("industry"),
    col("size_range"),
    extractUDF(col("locality")).alias("city"),
    col("total_employee_estimate").cast(IntegerType()),
)
tmp_companies_extracted.write.mode("overwrite").option("delimiter", ",").csv(
    "/spark/1/tmp_companies_extracted"
)
spark.stop()
