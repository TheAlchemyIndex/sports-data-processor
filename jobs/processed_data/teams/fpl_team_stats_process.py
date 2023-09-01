from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_fixtures_path = ConfigurationParser.get_config(
    "file_paths", "processed_fixtures_output"
)
_processed_teams_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_teams_output"
)


def run():
    job_name = "fpl_team_stats_process"

    spark, log = start_spark(app_name=job_name, files=[])
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
    fixtures_df = spark.read.format("parquet").load(
        f"{_bucket}/{_processed_data_output_path}/{_processed_fixtures_path}/season={_season}"
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
        .withColumn("date", fn.to_date("kickoff_time"))
        .drop("kickoff_time")
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
        .save(f"{_bucket}/{_processed_data_output_path}/{_processed_teams_output_path}/season={_season}")
    )


if __name__ == "__main__":
    run()
