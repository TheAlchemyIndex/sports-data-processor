from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run():
    job_name = "season_averages"

    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        processed_fixtures_df, player_averages_df, teams_averages_df = extract_data(
            spark
        )
        players_fixtures_teams_df = transform_data(
            processed_fixtures_df, player_averages_df, teams_averages_df
        )
        load_data(players_fixtures_teams_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed players data.
    """
    processed_fixtures_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/fixtures/season={_season}/fpl/")
        .filter(fn.col("kickoff_time").isNotNull())
        .withColumn("date", fn.to_date(fn.col("kickoff_time"), "yyyy-MM-dd"))
        .withColumnRenamed("team_h", "team")
        .withColumnRenamed("team_a", "opponent")
        .select("event", "date", "team", "opponent")
    )

    players_averages_df = spark.read.format("parquet").load(
        f"{_bucket}/averages/players/"
    )

    teams_averages_df = spark.read.format("parquet").load(f"{_bucket}/averages/teams/")

    return processed_fixtures_df, players_averages_df, teams_averages_df


def transform_data(fixtures_df, player_averages_df, teams_averages_df):
    """
    Transform processed fixtures data.
    """
    fixtures_reversed_df = (
        fixtures_df.withColumnRenamed("team", "opponent_temp")
        .withColumnRenamed("opponent", "team")
        .withColumnRenamed("opponent_temp", "opponent")
        .withColumn("team_type", fn.lit("a"))
    )

    all_teams_fixtures_df = (
        fixtures_df.withColumn("team_type", fn.lit("h"))
        .unionByName(fixtures_reversed_df)
        .withColumn(
            "opponent_type", fn.when(fn.col("team_type") == "h", "a").otherwise("h")
        )
    )

    players_fixtures_df = (
        player_averages_df.withColumn(
            "team_type", fn.when(fn.col("was_home"), "h").otherwise("a")
        )
        .drop(fn.col("was_home"))
        .join(all_teams_fixtures_df, on=["team", "team_type"], how="full")
    )

    teams_averages_df = (
        teams_averages_df.withColumnRenamed("team", "opponent")
        .withColumnRenamed("team_type", "opponent_type")
        .withColumnRenamed("goals_scored_avg", "opponent_goals_scored_avg")
        .withColumnRenamed("goals_conceded_avg", "opponent_goals_conceded_avg")
    )

    players_fixtures_teams_df = players_fixtures_df.join(
        teams_averages_df, on=["opponent", "opponent_type"], how="full"
    )

    return players_fixtures_teams_df


def load_data(players_fixtures_teams_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_fixtures_teams_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/averages/season/")
    )
