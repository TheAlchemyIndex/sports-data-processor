from pyspark.sql import functions as fn

from dependencies.spark import start_spark

JOB_NAME = "fpl_team_stats_process"
OUTPUT_PATH = "C:/repos/sports-data-processor/data/football/processed-data/teams"


def main():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        fixtures_df = extract_data(spark)
        team_stats_df = transform_data(fixtures_df)
        load_data(team_stats_df)
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
        spark.read.format("parquet")
        .load("C:/repos/sports-data-processor/data/football/processed-data/fixtures/")
    )

    return fixtures_df


def transform_data(fixtures_df):
    """
    Transform fixtures data.
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
        team_stats_df
        .write.format("parquet")
        .partitionBy("season", "team")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
