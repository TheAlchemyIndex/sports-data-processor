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
from dependencies.spark import start_spark

JOB_NAME = "fpl_fixtures_ingest"
SEASON = "2023-24"
FIXTURES_ENDPOINT = "https://fantasy.premierleague.com/api/fixtures/"
OUTPUT_PATH = (
    f"C:/sports-data-processor/football/fpl-ingest/fixtures/season={SEASON}"
)

STATS_SCHEMA = StructType(
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

FIXTURES_SCHEMA = StructType(
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
        StructField("stats", ArrayType(STATS_SCHEMA), True),
        StructField("team_h_difficulty", IntegerType(), True),
        StructField("team_a_difficulty", IntegerType(), True),
        StructField("pulse_id", IntegerType(), True),
    ]
)


def run():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        fixtures_raw_data = extract_data()
        fixtures_df = transform_data(fixtures_raw_data, spark)
        load_data(fixtures_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data():
    """
    Gets fixtures data from FPL API.
    """
    response = requests.get(FIXTURES_ENDPOINT)
    response.raise_for_status()
    fixtures_data = json.loads(response.text)
    return fixtures_data


def transform_data(fixtures_data, spark):
    """
    Transform json data into a DataFrame.
    """
    fixtures_df = spark.createDataFrame(fixtures_data, FIXTURES_SCHEMA).withColumn(
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
        .save(OUTPUT_PATH)
    )


if __name__ == "__main__":
    run()
