from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session
from dependencies.current_gw import get_current_gw

_current_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def get_previous_season():
    split_season = _current_season.split("-")
    previous_season_start = int(split_season[0]) - 1
    previous_season_end = int(split_season[1]) - 1
    return str(f"{previous_season_start}-{previous_season_end}")


def run():
    job_name = "fpl_player_name_validator"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        elements_ingest_df, teams_ingest_df = extract_data(spark)
        players_attributes_df = transform_data(elements_ingest_df, teams_ingest_df)
        load_data(players_attributes_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed player attributes and previous season elements data.
    """
    current_season_players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/attributes/")
        .filter(fn.col("season") == _current_season)
        # .filter(fn.col("round") == get_current_gw())
        .filter(fn.col("round") == 12)
    )

    previous_season_players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/attributes/")
        .filter(fn.col("season") == get_previous_season())
        .filter(fn.col("round") == 38)
    )

    return current_season_players_attributes_df, previous_season_players_attributes_df


def transform_data(
    current_season_players_attributes_df, previous_season_players_attributes_df
):
    """
    Join current season attributes and previous season attributes together to get missing players.
    """
    joined_attributes_df = current_season_players_attributes_df.join(
        previous_season_players_attributes_df, on=["name"], how="leftanti"
    )

    return joined_attributes_df


def load_data(joined_attributes_df):
    """
    Display results of joining attributes data together.
    """
    joined_attributes_df.show()
