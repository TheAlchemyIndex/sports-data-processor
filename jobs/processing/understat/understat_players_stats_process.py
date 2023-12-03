from pyspark.sql import functions as fn, Window

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
    job_name = "understat_players_stats_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        understat_players_df, players_attributes_df, missing_players_df = extract_data(spark)
        processed_understat_df = transform_data(
            understat_players_df, players_attributes_df, missing_players_df
        )
        load_data(processed_understat_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets understat players raw ingress data.
    """
    understat_players_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/understat/players/")
        .filter(fn.col("season") == get_previous_season())
        .filter(fn.col("league") != "EPL")
    ).drop("id", "position")

    understat_players_df.show()

    players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/attributes/")
        .filter(fn.col("season") == _season)
        # .filter(fn.col("round") == get_current_gw())
        .filter(fn.col("round") == 13)
        .select("id", "name", "position")
    )

    players_attributes_df.show()

    missing_players_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/missing-players/")
        .filter(fn.col("season") == _season)
        .select("name")
    )

    missing_players_df.show()

    return understat_players_df, players_attributes_df, missing_players_df


def transform_data(understat_players_df, players_attributes_df, missing_players_df):
    # Gets the player's team by getting the most frequent value in the h_team column
    window = Window.partitionBy("name", "h_team").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    processed_understat_players_df = (
        understat_players_df
        .withColumn("team_count", fn.count("h_team").over(window))
        .withColumn("total_points", fn.lit(0))
        .withColumn(
            "was_home", fn.when(fn.col("team_count") != 1, True).otherwise(False)
        )
        .withColumn("kickoff_time", fn.concat(fn.col("date"), fn.lit("T00:00:00Z")))
        .withColumn(
            "goals_conceded",
            fn.when(fn.col("was_home"), fn.col("a_goals")).otherwise(fn.col("h_goals")),
        )
        .withColumn(
            "clean_sheets",
            fn.when(
                (fn.col("time") >= 60) & (fn.col("goals_conceded") == 0), 1
            ).otherwise(0),
        )
        .withColumn("yellow_cards", fn.lit(0))
        .withColumn("saves", fn.lit(0))
        .withColumn("bonus", fn.lit(0))
        .withColumn("value", fn.lit(0))
        .withColumn(
            "opponent_team",
            fn.when(fn.col("was_home"), fn.col("a_team")).otherwise(fn.col("h_team")),
        )
        .withColumn("first_name", fn.split(fn.col("name"), " ").getItem(0))
        .withColumn("second_name", fn.expr("concat_ws(' ', slice(split(name, ' '), 2, size(split(name, ' '))))"))
        .withColumn("chance_of_playing_next_round", fn.lit(0))
        .withColumn("news", fn.lit(""))
        .withColumn("team", fn.when(fn.col("team_count") != 1, fn.col("h_team")).otherwise(fn.col("a_team")))
    )

    understat_with_attributes_df = (
        processed_understat_players_df.join(
            players_attributes_df, on="name", how="inner"
        )
    ).select(
        "id",
        "total_points",
        "was_home",
        "kickoff_time",
        fn.col("time").alias("minutes"),
        fn.col("goals").alias("goals_scored"),
        "assists",
        "clean_sheets",
        "goals_conceded",
        "yellow_cards",
        "saves",
        "bonus",
        "value",
        "opponent_team",
        "first_name",
        "second_name",
        "chance_of_playing_next_round",
        "news",
        "position",
        "team",
        "name",
    )

    understat_filtered_df = (
        understat_with_attributes_df.join(
            missing_players_df, on="name", how="inner"
        )
    ).withColumn("source", fn.lit("understat"))

    return understat_filtered_df


def load_data(understat_filtered_df):
    """
    Write DataFrame as Parquet format.
    """
    # (
    #     understat_filtered_df.write.format("parquet")
    #     # .partitionBy("source", "name", "round")
    #     .partitionBy("source", "name")
    #     .mode("append")
    #     .save(f"{_bucket}/processed-ingress/players/stats/season={get_previous_season()}")
    # )

    (
        understat_filtered_df.write.format("parquet")
        # .partitionBy("source", "name", "round")
        .partitionBy("source", "name")
        .mode("append")
        .save(f"{_bucket}/processed-ingress/test")
    )
