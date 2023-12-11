from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session
from dependencies.current_gw import get_current_gw

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def get_previous_season():
    split_season = _season.split("-")
    previous_season_start = int(split_season[0]) - 1
    previous_season_end = int(split_season[1]) - 1
    return str(f"{previous_season_start}-{previous_season_end}")


def run():
    job_name = "fpl_player_name_validator"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        players_attributes_df, previous_players_stats_df = extract_data(spark)
        missing_players_df = transform_data(
            players_attributes_df, previous_players_stats_df
        )
        load_data(missing_players_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed player attributes and previous season processed stats data.
    """
    current_season_players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/attributes/")
        .filter(fn.col("season") == _season)
        .filter(fn.col("round") == get_current_gw())
        .select("id", "name", "position", "team")
    )

    previous_season_players_stats_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/stats/")
        .filter(fn.col("season") == get_previous_season())
        .filter(fn.col("round") == 38)
        .select("name")
    )

    return current_season_players_attributes_df, previous_season_players_stats_df


def transform_data(
    current_season_players_attributes_df, previous_season_players_stats_df
):
    """
    Join current season attributes and previous season stats together to get missing players.
    """
    missing_players_df = current_season_players_attributes_df.join(
        previous_season_players_stats_df, on=["name"], how="leftanti"
    )

    return missing_players_df


def load_data(missing_players_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        missing_players_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/processed-ingress/players/missing-players/season={_season}")
    )


if __name__ == "__main__":
    run()
