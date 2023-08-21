import pyspark.sql.functions as fn
from dependencies.spark import start_spark

JOB_NAME = "gw_csv_formatter"
SEASON = "2022-23"
OUTPUT_PATH = f"C:/repos/sports-data-processor/data/football/fpl-ingest/fixtures/season={SEASON}/"


def main():
    spark, log, config = start_spark(
        app_name=JOB_NAME,
        files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        gw_csv_data, teams_data = extract_data(spark)
        joined_df = transform_data(gw_csv_data, teams_data)
        load_data(joined_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Reads data from csv file.
    """
    fixtures_csv_df = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ",")
        .load("C:/repos/sports-data-processor/data/fixtures.csv")
    )

    teams_df = (
        spark
        .read
        .format("parquet")
        .load(f"C:/repos/sports-data-processor/data/football/fpl-ingest/teams/season={SEASON}")
        .withColumn("away_team", fn.col("name"))
        .withColumnRenamed("name", "home_team")
        .select("home_team", "away_team", "id")
    )

    return fixtures_csv_df, teams_df


def transform_data(gw_csv_df, teams_df):
    """
    Transform json data into a DataFrame.
    """
    joined_df = (
        gw_csv_df
        .join(teams_df.drop("away_team").withColumnRenamed("id", "team_h"),
              on=["home_team"],
              how="inner")
        .drop("home_team")
    )

    joined_df = (
        joined_df
        .join(teams_df.drop("home_team").withColumnRenamed("id", "team_a"),
              on=["away_team"],
              how="inner")
        .drop("away_team")
        .select("code", "event", "finished", "finished_provisional", "id", "kickoff_time", "minutes",
                "provisional_start_time", "started", "team_a", "team_a_score", "team_h", "team_h_score", "stats",
                "team_h_difficulty", "team_a_difficulty", "pulse_id")
    )

    return joined_df


def load_data(df):
    """
    Write DataFrame as Parquet format.
    """
    (
        df
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(OUTPUT_PATH)
    )


if __name__ == '__main__':
    main()
