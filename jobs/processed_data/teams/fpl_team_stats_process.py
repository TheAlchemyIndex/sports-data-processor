from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run():
    job_name = "fpl_team_stats_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        fixtures_processed_df = extract_data(spark)
        team_stats_df = transform_data(fixtures_processed_df)
        load_data(team_stats_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed fixtures data.
    """
    fixtures_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/fixtures/")
        .filter(fn.col("season") == _season)
    )

    return fixtures_df


def transform_data(fixtures_df):
    """
    Transform processed fixtures data.
    """
    home_fixtures_df = (
        fixtures_df.drop("team_a")
        .withColumnRenamed("team_h", "team")
        .withColumnRenamed("team_h_score", "goals_scored")
        .withColumnRenamed("team_a_score", "goals_conceded")
        .withColumn("team_type", fn.lit("h"))
    )

    away_fixtures_df = (
        fixtures_df.drop("team_h")
        .withColumnRenamed("team_a", "team")
        .withColumnRenamed("team_a_score", "goals_scored")
        .withColumnRenamed("team_h_score", "goals_conceded")
        .withColumn("team_type", fn.lit("a"))
    )

    team_stats_df = (
        home_fixtures_df.unionByName(away_fixtures_df)
        .withColumnRenamed("kickoff_time", "date")
        .withColumn("date", fn.to_date("date"))
        # To only return fixtures that have taken place
        .filter(fn.col("date") < fn.current_date())
    )

    return team_stats_df


def load_data(team_stats_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        team_stats_df.write.format("parquet")
        .partitionBy("team")
        .mode("overwrite")
        .save(f"{_bucket}/processed-ingress/teams/season={_season}/")
    )
