from dependencies.spark import start_spark
from jobs.processed_data.averages.average_calculator import last_n_rows, calculate_partitioned_avg

JOB_NAME = "teams_average_processor"
SEASON = "2023-24"
OUTPUT_PATH = "C:/sports-data-processor/football/processed-data/averages/teams"


def main():
    spark, log = start_spark(
        app_name=JOB_NAME,
        files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        teams_df = extract_data(spark)
        last_five_rows_avg_df = transform_data(teams_df)
        load_data(last_five_rows_avg_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets teams data.
    """
    teams_df = (
        spark
        .read
        .format("parquet")
        .load("C:/sports-data-processor/football/raw-ingress/teams")
    )

    return teams_df


def transform_data(teams_df):
    """
    Transform teams data.
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
        .save(f"{OUTPUT_PATH}")
    )


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
