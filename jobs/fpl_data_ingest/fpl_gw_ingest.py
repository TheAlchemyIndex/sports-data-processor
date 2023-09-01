import json
import requests
import pyspark.sql.functions as fn
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
    TimestampType,
)

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_fpl_ingest_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_ingest_output"
)
_fpl_gws_output_path = ConfigurationParser.get_config("file_paths", "fpl_gws_output")
_fpl_player_history_endpoint = ConfigurationParser.get_config(
    "external", "fpl_player_history_uri"
)

_history_schema = StructType(
    [
        StructField("element", IntegerType(), True),
        StructField("fixture", IntegerType(), True),
        StructField("opponent_team", IntegerType(), True),
        StructField("total_points", IntegerType(), True),
        StructField("was_home", BooleanType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("team_h_score", IntegerType(), True),
        StructField("team_a_score", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("minutes", IntegerType(), True),
        StructField("goals_scored", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("clean_sheets", IntegerType(), True),
        StructField("goals_conceded", IntegerType(), True),
        StructField("own_goals", IntegerType(), True),
        StructField("penalties_saved", IntegerType(), True),
        StructField("penalties_missed", IntegerType(), True),
        StructField("yellow_cards", IntegerType(), True),
        StructField("red_cards", IntegerType(), True),
        StructField("saves", IntegerType(), True),
        StructField("bonus", IntegerType(), True),
        StructField("bps", IntegerType(), True),
        StructField("influence", StringType(), True),
        StructField("creativity", StringType(), True),
        StructField("threat", StringType(), True),
        StructField("ict_index", StringType(), True),
        StructField("starts", IntegerType(), True),
        StructField("expected_goals", StringType(), True),
        StructField("expected_assists", StringType(), True),
        StructField("expected_goal_involvements", StringType(), True),
        StructField("expected_goals_conceded", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("transfers_balance", IntegerType(), True),
        StructField("selected", IntegerType(), True),
        StructField("transfers_in", IntegerType(), True),
        StructField("transfers_out", IntegerType(), True),
    ]
)


def run():
    job_name = "fpl_gw_ingest"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        gw_raw_data = extract_data(spark)
        gw_df = transform_data(gw_raw_data, spark)
        load_data(gw_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets player gameweek data from FPL API, using player ids from elements data.
    """
    # TODO Work out a better way to get current gameweek that can be used across other jobs
    events_response = requests.get(
        "https://fantasy.premierleague.com/api/bootstrap-static/"
    )
    events_response.raise_for_status()
    events_data = json.loads(events_response.text)["events"]
    gw_num = 0
    for event in events_data:
        if event["is_current"]:
            gw_num = event["id"]

    # Elements data, which contains column of player ids
    elements_df = (
        spark.read.format("parquet")
        .load(
            f"C:/sports-data-processor/football/fpl-ingest/players/elements/season={_season}"
        )
        .select("id")
    )

    id_list = elements_df.select(fn.collect_list("id")).first()[0]

    # Temp dictionary to hold gameweek data for each player
    gw_data = {"history": []}

    # TODO Work out a better way to get current gameweek that can be used across other jobs
    for element_id in id_list:
        current_gw_response = requests.get(
            f"{_fpl_player_history_endpoint}{element_id}"
        )
        current_gw_response.raise_for_status()
        history_data = json.loads(current_gw_response.text)["history"]
        for gameweek in history_data:
            if gameweek["round"] == gw_num:
                gw_data["history"].extend([gameweek])

    return gw_data["history"]


def transform_data(gw_data, spark):
    """
    Transform gameweek data into a DataFrame.
    """
    gw_df = spark.createDataFrame(gw_data, _history_schema).withColumn(
        "kickoff_time",
        fn.from_utc_timestamp(fn.col("kickoff_time"), "UTC").cast(TimestampType()),
    )
    return gw_df


def load_data(gw_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        gw_df.coalesce(1)
        .write.format("parquet")
        .partitionBy("round")
        .mode("append")
        .save(
            f"{_bucket}/{_fpl_ingest_output_path}/{_fpl_gws_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
