import json
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
)

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_fpl_ingest_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_ingest_output"
)
_fpl_teams_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_teams_output"
)
_fpl_teams_endpoint = ConfigurationParser.get_config("external", "fpl_main_uri")

_teams_schema = StructType(
    [
        StructField("code", IntegerType(), True),
        StructField("draw", IntegerType(), True),
        StructField("form", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("loss", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("played", IntegerType(), True),
        StructField("points", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("short_name", StringType(), True),
        StructField("strength", IntegerType(), True),
        StructField("team_division", StringType(), True),
        StructField("unavailable", BooleanType(), True),
        StructField("win", IntegerType(), True),
        StructField("strength_overall_home", IntegerType(), True),
        StructField("strength_overall_away", IntegerType(), True),
        StructField("strength_attack_home", IntegerType(), True),
        StructField("strength_attack_away", IntegerType(), True),
        StructField("strength_defence_home", IntegerType(), True),
        StructField("strength_defence_away", IntegerType(), True),
        StructField("pulse_id", IntegerType(), True),
    ]
)


def run():
    jobs_name = "fpl_teams_ingest"

    spark, log = start_spark(app_name=jobs_name, files=[])
    log.warn(f"{jobs_name} running.")

    try:
        teams_raw_data = extract_data()
        teams_df = transform_data(teams_raw_data, spark)
        load_data(teams_df)
    except Exception as e:
        log.error(f"Error running {jobs_name}: {str(e)}")
    finally:
        log.warn(f"{jobs_name} is finished.")
        spark.stop()


def extract_data():
    """
    Gets teams data from FPL API.
    """
    response = requests.get(_fpl_teams_endpoint)
    response.raise_for_status()
    teams_data = json.loads(response.text)["teams"]
    return teams_data


def transform_data(teams_data, spark):
    """
    Transform json data into a DataFrame.
    """
    teams_df = spark.createDataFrame(teams_data, _teams_schema)
    return teams_df


def load_data(teams_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        teams_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/{_fpl_ingest_output_path}/{_fpl_teams_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
