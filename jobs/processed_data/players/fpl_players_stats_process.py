import json

import requests
from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

# _season = ConfigurationParser.get_config("external", "season")
_season = "2022-23"
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_players_stats_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_stats_output"
)
_processed_players_names_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_names_output"
)
_fpl_ingest_path = ConfigurationParser.get_config("file_paths", "fpl_ingest_output")
_fpl_gws_path = ConfigurationParser.get_config("file_paths", "fpl_gws_output")
_fpl_teams_path = ConfigurationParser.get_config("file_paths", "fpl_teams_output")
_fpl_events_endpoint = ConfigurationParser.get_config("external", "fpl_main_uri")


def run():
    job_name = "fpl_players_stats_process"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        for gw in range(1, 39):
            gws_ingest_df, teams_ingest_df, players_names_processed_df = extract_data(spark, gw)
            player_stats_df = transform_data(
                gws_ingest_df, teams_ingest_df, players_names_processed_df
            )
            load_data(player_stats_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark, gw_num):
    """
    Gets players ingest, teams ingest and player names processed data.
    """
    # TODO Work out a better way to get current gameweek that can be used across other jobs
    # events_response = requests.get(
    #     _fpl_events_endpoint
    # )
    # events_response.raise_for_status()
    # events_data = json.loads(events_response.text)["events"]
    # gw_num = 3
    # for event in events_data:
    #     if event["is_current"]:
    #         gw_num = event["id"]

    gws_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_gws_path}/")
        .filter(fn.col("season") == _season)
        .filter(fn.col("round") == gw_num)
        .withColumnRenamed("opponent_team", "opponent_id")
        .select(
            "element",
            "opponent_id",
            "total_points",
            "was_home",
            "kickoff_time",
            "minutes",
            "goals_scored",
            "assists",
            "clean_sheets",
            "goals_conceded",
            "yellow_cards",
            "saves",
            "bonus",
            "value",
            "season",
            "round",
        )
        .withColumnRenamed("element", "id")
    )

    teams_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_teams_path}/")
        .filter(fn.col("season") == _season)
        .withColumnRenamed("id", "opponent_id")
        .withColumnRenamed("name", "opponent_team")
        .select("opponent_id", "opponent_team", "season")
    )

    players_names_df = (
        spark.read.format("parquet")
        .load(
            f"{_bucket}/{_processed_data_output_path}/{_processed_players_names_output_path}/"
        )
        .filter(fn.col("season") == _season)
    )

    return gws_df, teams_df, players_names_df


def transform_data(gws_df, teams_df, players_names_df):
    players_with_teams_df = gws_df.join(
        teams_df, on=["opponent_id", "season"], how="inner"
    ).drop("opponent_id")

    players_with_names_df = players_with_teams_df.join(
        players_names_df, on=["id", "season"], how="inner"
    )

    return players_with_names_df


def load_data(players_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_names_df.write.format("parquet")
        .partitionBy("season", "name", "round")
        .mode("append")
        .save(
            f"{_bucket}/{_processed_data_output_path}/{_processed_players_stats_output_path}"
        )
    )


if __name__ == "__main__":
    run()
