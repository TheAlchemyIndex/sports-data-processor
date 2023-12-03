from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def get_previous_season():
    split_season = _season.split("-")
    previous_season_start = int(split_season[0]) - 1
    previous_season_end = int(split_season[1]) - 1
    return str(f"{previous_season_start}-{previous_season_end}")


def run():
    job_name = "fixture_downloader_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        fixtures_df = extract_data(spark)
        processed_fixtures_df = transform_data(fixtures_df)
        load_data(processed_fixtures_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Loads fixtures data.
    """
    fixtures_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fixture-downloader/")
        .filter(fn.col("season") == get_previous_season())
    )

    return fixtures_df


def transform_data(fixtures_df):
    """
    Transforms fixtures data.
    """
    processed_fixtures_df = (
        fixtures_df
        .select(
            fn.col("RoundNumber").alias("event"),
            fn.col("DateUtc").alias("kickoff_time"),
            fn.col("HomeTeam").alias("team_h"),
            fn.col("HomeTeamScore").alias("team_h_score"),
            fn.col("AwayTeamScore").alias("team_a"),
            fn.col("AwayTeamScore").alias("team_a_score"),
            "league"
        )
    )

    return processed_fixtures_df


def load_data(processed_fixtures_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        processed_fixtures_df.write.format("parquet")
        .partitionBy("league")
        .mode("overwrite")
        .save(f"{_bucket}/processed-ingress/fixtures/season={get_previous_season()}/fixture-downloader/")
    )


if __name__ == "__main__":
    run()
