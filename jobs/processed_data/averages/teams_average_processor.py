from pyspark.sql import functions as fn

from dependencies.spark import start_spark
from jobs.processed_data.averages.average_calculator import last_n_rows, calculate_partitioned_avg

JOB_NAME = "teams_average_processor"
SEASON = "2023-24"
OUTPUT_PATH = "C:/repos/sports-data-processor/data/football/processed-data/averages/teams"


def main():
    spark, log, config = start_spark(
        app_name=JOB_NAME,
        files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        fixtures_df, teams_df = extract_data(spark)
        fixtures_with_team_name_df = transform_data(fixtures_df, teams_df)
        load_data(fixtures_with_team_name_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets fixtures and teams data.
    """
    fixtures_df = (
        spark
        .read
        .format("parquet")
        .load("C:/repos/sports-data-processor/data/football/fpl-ingest/fixtures/")
        .select("event", "kickoff_time", "team_h", "team_h_score", "team_a", "team_a_score", "season")
    )

    teams_df = (
        spark
        .read
        .format("parquet")
        .load("C:/repos/sports-data-processor/data/football/fpl-ingest/teams/")
        .select("id", "name", "season")
    )

    return fixtures_df, teams_df


def transform_data(fixtures_df, teams_df):
    """
    Transform and join fixtures and teams data together.
    """
    home_fixtures_df = (
        fixtures_df
        .drop("team_a")
        .withColumnRenamed("team_h", "id")
        .withColumnRenamed("team_h_score", "goals_scored")
        .withColumnRenamed("team_a_score", "goals_conceded")
        .withColumn("team_type", fn.lit("h"))
    )

    away_fixtures_df = (
        fixtures_df
        .drop("team_h")
        .withColumnRenamed("team_a", "id")
        .withColumnRenamed("team_a_score", "goals_scored")
        .withColumnRenamed("team_h_score", "goals_conceded")
        .withColumn("team_type", fn.lit("a"))
    )

    all_fixtures_df = (
        home_fixtures_df
        .unionByName(away_fixtures_df)
        .withColumn("date", fn.to_date("kickoff_time"))
        .drop("kickoff_time")
    )

    fixtures_with_team_name_df = (
        all_fixtures_df
        .join(teams_df,
              on=["id", "season"],
              how="inner")
        .drop("id")
        .withColumnRenamed("name", "team")
        .filter(fn.col("date") < fn.current_date())
    )

    last_five_rows_df = last_n_rows(fixtures_with_team_name_df, "team_type", "team", 5)

    last_five_rows_avg_df = (
        last_five_rows_df
        .withColumn("goals_scored_avg", calculate_partitioned_avg("team_type", "team", "goals_scored"))
        .withColumn("goals_conceded_avg", calculate_partitioned_avg("team_type", "team", "goals_conceded"))
        .select("team", "team_type", "goals_scored_avg", "goals_conceded_avg")
        .dropDuplicates()
    )

    return last_five_rows_avg_df


def load_data(fixtures_with_team_name_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        fixtures_with_team_name_df
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
