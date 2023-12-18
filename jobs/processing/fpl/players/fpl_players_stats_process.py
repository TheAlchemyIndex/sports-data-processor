from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session
from dependencies.gw_getter import get_current_gw

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run():
    job_name = "fpl_players_stats_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        gws_ingest_df, teams_ingest_df, players_attributes_processed_df = extract_data(
            spark
        )
        player_stats_df = transform_data(
            gws_ingest_df, teams_ingest_df, players_attributes_processed_df
        )
        load_data(player_stats_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets players ingest, teams ingest and player names processed data.
    """
    gws_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/players/rounds/")
        .filter(fn.col("season") == _season)
        .filter(fn.col("round") == get_current_gw())
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
        )
        .withColumnRenamed("element", "id")
    )

    teams_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/teams/")
        .filter(fn.col("season") == _season)
        .withColumnRenamed("id", "opponent_id")
        .withColumnRenamed("name", "opponent_team")
        .select("opponent_id", "opponent_team")
    )

    players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/attributes/")
        .filter(fn.col("season") == _season)
        .filter(fn.col("round") == get_current_gw())
        .drop("season")
    )

    return gws_df, teams_df, players_attributes_df


def transform_data(gws_df, teams_df, players_attributes_df):
    players_with_teams_df = gws_df.join(teams_df, on="opponent_id", how="inner").drop(
        "opponent_id"
    )

    players_with_names_df = players_with_teams_df.join(
        players_attributes_df, on="id", how="inner"
    )

    return players_with_names_df


def load_data(players_stats_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_stats_df.write.format("parquet")
        .partitionBy("name", "round")
        .mode("append")
        .save(f"{_bucket}/processed-ingress/players/stats/season={_season}/source=fpl/")
    )
