from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_fpl_ingest_path = ConfigurationParser.get_config("file_paths", "fpl_ingest_output")
_fpl_fixtures_path = ConfigurationParser.get_config("file_paths", "fpl_fixtures_output")
_fpl_teams_path = ConfigurationParser.get_config("file_paths", "fpl_teams_output")
_processed_data_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_fixtures_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_fixtures_output"
)


def run():
    job_name = "fpl_fixtures_ingress"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        fixtures_ingest_df, teams_ingest_df = extract_data(spark)
        fixtures_with_team_names_df = transform_data(
            fixtures_ingest_df, teams_ingest_df
        )
        load_data(fixtures_with_team_names_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets fixtures and teams data.
    """
    fixtures_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_fixtures_path}/")
        .select(
            "event",
            "kickoff_time",
            "team_h",
            "team_h_score",
            "team_a",
            "team_a_score",
            "season",
        )
        .filter(fn.col("season") == _season)
    )

    teams_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_teams_path}/")
        .select("id", "name", "season")
        .filter(fn.col("season") == _season)
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
        .drop("id", "name")
        .drop(fixtures_df["season"])
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
        .save(
            f"{_bucket}/{_processed_data_output_path}/{_processed_fixtures_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
