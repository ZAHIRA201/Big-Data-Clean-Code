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
from fuzzywuzzy import fuzz


def fuzzy_match(s1: str, s2: str) -> bool:
    match_ratio = fuzz.ratio(s1, s2)
    match_weight = fuzz.WRatio(s1, s2)
    match_token = fuzz.partial_token_sort_ratio(s1, s2)
    return (match_ratio > 83 or match_weight > 86 or match_token > 88) and not (
        match_ratio < 65 or match_token < 55
    )


fuzzy_matchUDF = udf(lambda z1, z2: fuzzy_match(z1, z2), BooleanType())
spark = SparkSession.builder.appName("tmp_mix").getOrCreate()
tmp_companies = (
    spark.read.option("delimiter", ",")
    .option("escapeQuotes", "true")
    .csv(
        "/spark/1/tmp_companies_extracted",
        schema=StructType(
            [
                StructField("name", StringType()),
                StructField("industry", StringType()),
                StructField("size_range", StringType()),
                StructField("city", StringType()),
                StructField("total_employee_estimate", IntegerType()),
            ]
        ),
    )
)
tmp_injuries = (
    spark.read.option("delimiter", ",")
    .option("escapeQuotes", "true")
    .csv(
        "/spark/1/tmp_injuries_filtered",
        schema=StructType(
            [
                StructField("company_name", StringType()),
                StructField("establishment_name", StringType()),
                StructField("city", StringType()),
                StructField("total_deaths", IntegerType()),
                StructField("total_injuries", IntegerType()),
            ]
        ),
    )
)
tmp_mix = (
    tmp_injuries.join(tmp_companies, "city")
    .filter(fuzzy_matchUDF(tmp_injuries["company_name"], tmp_companies["name"]) == True)
    .select(
        tmp_injuries["company_name"],
        tmp_companies["industry"],
        tmp_companies["size_range"],
        tmp_injuries["city"],
        tmp_injuries["total_deaths"],
        tmp_injuries["total_injuries"],
        tmp_companies["total_employee_estimate"],
    )
)
tmp_mix.write.mode("overwrite").option("delimiter", ",").csv("/spark/1/tmp_mix")
spark.stop()
