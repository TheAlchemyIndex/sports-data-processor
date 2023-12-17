from pyspark.sql import functions as fn
from config import ConfigurationParser
from dependencies.spark import create_spark_session
from jobs.averages.util.average_calculator import last_n_rows, calculate_partitioned_avg

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run():
    job_name = "teams_averages"

    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        teams_df, current_season_teams_df = extract_data(spark)
        last_five_rows_avg_df = transform_data(teams_df, current_season_teams_df)
        load_data(last_five_rows_avg_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed teams data.
    """
    teams_df = spark.read.format("parquet").load(f"{_bucket}/processed-ingress/teams/")

    current_season_teams_df = (
        teams_df.filter(fn.col("season") == _season).select("team").dropDuplicates()
    )

    return teams_df.drop("event", "season"), current_season_teams_df


def transform_data(teams_df, current_season_teams_df):
    """
    Transform processed teams data.
    """
    filter_current_teams_df = teams_df.join(
        current_season_teams_df, on="team", how="inner"
    )

    last_five_rows_df = last_n_rows(filter_current_teams_df, "team_type", "team", 5)

    last_five_rows_avg_df = (
        last_five_rows_df.withColumn(
            "goals_scored_avg",
            calculate_partitioned_avg("team_type", "team", "goals_scored"),
        )
        .withColumn(
            "goals_conceded_avg",
            calculate_partitioned_avg("team_type", "team", "goals_conceded"),
        )
        .select("team", "team_type", "goals_scored_avg", "goals_conceded_avg")
        .dropDuplicates()
    )

    return last_five_rows_avg_df


def load_data(last_five_rows_avg_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        last_five_rows_avg_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/averages/teams/")
    )
