import json
import requests
from pyspark.sql import functions as fn
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
    ArrayType,
    TimestampType,
)

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_fpl_ingest_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_ingest_output"
)
_fpl_fixtures_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_fixtures_output"
)
_fpl_fixtures_endpoint = ConfigurationParser.get_config("external", "fpl_fixtures_uri")

_stats_schema = StructType(
    [
        StructField("identifier", StringType(), False),
        StructField(
            "a",
            ArrayType(
                StructType(
                    [
                        StructField("value", IntegerType(), False),
                        StructField("element", IntegerType(), False),
                    ]
                )
            ),
        ),
        StructField(
            "h",
            ArrayType(
                StructType(
                    [
                        StructField("value", IntegerType(), False),
                        StructField("element", IntegerType(), False),
                    ]
                )
            ),
        ),
    ]
)

_fixtures_schema = StructType(
    [
        StructField("code", IntegerType(), True),
        StructField("event", IntegerType(), True),
        StructField("finished", BooleanType(), True),
        StructField("finished_provisional", BooleanType(), True),
        StructField("id", IntegerType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("minutes", IntegerType(), True),
        StructField("provisional_start_time", BooleanType(), True),
        StructField("started", BooleanType(), True),
        StructField("team_a", IntegerType(), True),
        StructField("team_a_score", IntegerType(), True),
        StructField("team_h", IntegerType(), True),
        StructField("team_h_score", IntegerType(), True),
        StructField("stats", ArrayType(_stats_schema), True),
        StructField("team_h_difficulty", IntegerType(), True),
        StructField("team_a_difficulty", IntegerType(), True),
        StructField("pulse_id", IntegerType(), True),
    ]
)


def run():
    job_name = "fpl_fixtures_ingest"

    spark, log = create_spark_session(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        fixtures_raw_data = extract_data()
        fixtures_df = transform_data(fixtures_raw_data, spark)
        load_data(fixtures_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data():
    """
    Gets fixtures data from FPL API.
    """
    response = requests.get(_fpl_fixtures_endpoint)
    response.raise_for_status()
    fixtures_data = json.loads(response.text)
    return fixtures_data


def transform_data(fixtures_data, spark):
    """
    Transform json data into a DataFrame.
    """
    fixtures_df = spark.createDataFrame(fixtures_data, _fixtures_schema).withColumn(
        "kickoff_time",
        fn.from_utc_timestamp(fn.col("kickoff_time"), "UTC").cast(TimestampType()),
    )

    return fixtures_df


def load_data(fixtures_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        fixtures_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/{_fpl_ingest_output_path}/{_fpl_fixtures_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
