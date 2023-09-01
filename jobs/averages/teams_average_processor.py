from config import ConfigurationParser
from dependencies.spark import start_spark
from jobs.averages.average_calculator import last_n_rows, calculate_partitioned_avg

_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_teams_path = ConfigurationParser.get_config(
    "file_paths", "processed_teams_output"
)
_teams_average_output_path = ConfigurationParser.get_config(
    "file_paths", "teams_average_output"
)


def run():
    job_name = "teams_average_processor"

    spark, log = start_spark(
        app_name=job_name,
        files=[])
    log.warn(f"{job_name} running.")

    try:
        teams_df = extract_data(spark)
        last_five_rows_avg_df = transform_data(teams_df)
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
    teams_df = (
        spark
        .read
        .format("parquet")
        .load(f"{_bucket}/{_processed_data_path}/{_processed_teams_path}")
    )

    return teams_df


def transform_data(teams_df):
    """
    Transform processed teams data.
    """
    last_five_rows_df = last_n_rows(teams_df, "team_type", "team", 5)

    last_five_rows_avg_df = (
        last_five_rows_df
        .withColumn("goals_scored_avg", calculate_partitioned_avg("team_type", "team", "goals_scored"))
        .withColumn("goals_conceded_avg", calculate_partitioned_avg("team_type", "team", "goals_conceded"))
        .select("team", "team_type", "goals_scored_avg", "goals_conceded_avg")
        .dropDuplicates()
    )

    return last_five_rows_avg_df


def load_data(last_five_rows_avg_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        last_five_rows_avg_df
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/{_teams_average_output_path}")
    )


if __name__ == '__main__':
    run()