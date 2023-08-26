from pyspark.sql import functions as fn

from dependencies.spark import start_spark

JOB_NAME = "fpl_fixtures_ingress"
OUTPUT_PATH = "C:/sports-data-processor/football/processed-data/fixtures"


def run():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        fixtures_ingest_df, teams_ingest_df = extract_data(spark)
        fixtures_with_team_names_df = transform_data(
            fixtures_ingest_df, teams_ingest_df
        )
        load_data(fixtures_with_team_names_df)
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
        .load("C:/sports-data-processor/football/fpl-ingest/fixtures/")
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
        spark.read.format("parquet")
        .load("C:/sports-data-processor/football/fpl-ingest/teams/")
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
        .drop("id", "name")
        .drop(fixtures_with_home_team_names_df["season"])
    )

    return fixtures_with_away_team_names_df


def load_data(fixtures_with_team_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        fixtures_with_team_names_df.write.format("parquet")
        .partitionBy("season")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


if __name__ == "__main__":
    run()
