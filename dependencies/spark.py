import os
from config import ConfigurationParser
from pyspark.sql import SparkSession
from dependencies import logging

os.environ["AWS_ACCESS_KEY_ID"] = ConfigurationParser.get_config(
    "aws_keys", "aws_access_key_id"
)
os.environ["AWS_SECRET_ACCESS_KEY"] = ConfigurationParser.get_config(
    "aws_keys", "aws_secret_access_key"
)


def create_spark_session(app_name="sports_data_processor", master="local[*]"):
    spark = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    spark_logger = logging.Log4j(spark)

    return spark, spark_logger
