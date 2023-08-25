import json
import requests
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
)
from dependencies.spark import start_spark

JOB_NAME = "fpl_teams_ingest"
SEASON = "2023-24"
TEAMS_ENDPOINT = "https://fantasy.premierleague.com/api/bootstrap-static/"
OUTPUT_PATH = (
    f"C:/repos/sports-data-processor/data/football/fpl-ingest/teams/season={SEASON}"
)

TEAMS_SCHEMA = StructType(
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
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        teams_raw_data = extract_data()
        teams_df = transform_data(teams_raw_data, spark)
        load_data(teams_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data():
    """
    Gets teams data from FPL API.
    """
    response = requests.get(TEAMS_ENDPOINT)
    response.raise_for_status()
    teams_data = json.loads(response.text)["teams"]
    return teams_data


def transform_data(teams_data, spark):
    """
    Transform json data into a DataFrame.
    """
    teams_df = spark.createDataFrame(teams_data, TEAMS_SCHEMA)
    return teams_df


def load_data(teams_df):
    """
    Write DataFrame as Parquet format.
    """
    (teams_df.coalesce(1).write.format("parquet").mode("overwrite").save(OUTPUT_PATH))


if __name__ == "__main__":
    run()
