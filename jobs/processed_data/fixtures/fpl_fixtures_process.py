from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run():
    job_name = "fpl_fixtures_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        fixtures_df, teams_df = extract_data(spark)
        fixtures_with_team_names_df = transform_data(fixtures_df, teams_df)
        load_data(fixtures_with_team_names_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Loads fixtures and teams data.
    """
    fixtures_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/fixtures/")
        .filter(fn.col("season") == _season)
        .select(
            "event",
            "kickoff_time",
            "team_h",
            "team_h_score",
            "team_a",
            "team_a_score",
            "season",
        )
    )

    teams_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/teams")
        .filter(fn.col("season") == _season)
        .select("id", "name", "season")
    )

    return fixtures_df, teams_df


def transform_data(fixtures_df, teams_df):
    """
    Transforms and join fixtures and teams data together.
    """
    fixtures_with_home_team_names_df = (
        fixtures_df.join(
            teams_df,
            (fixtures_df["team_h"] == teams_df["id"])
            & (fixtures_df["season"] == teams_df["season"]),
            how="inner",
        )
        .withColumn("team_h", fn.col("name"))
        .drop("id", "name", fixtures_df["season"])
    )

    fixtures_with_away_team_names_df = (
        fixtures_with_home_team_names_df.join(
            teams_df,
            (fixtures_with_home_team_names_df["team_a"] == teams_df["id"])
            & (fixtures_with_home_team_names_df["season"] == teams_df["season"]),
            how="inner",
        )
        .withColumn("team_a", fn.col("name"))
        .drop("id", "name", "season")
    )

    return fixtures_with_away_team_names_df


def load_data(fixtures_with_team_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        fixtures_with_team_names_df.write.format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/processed-ingress/fixtures/season={_season}/")
    )
