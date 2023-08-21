from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, LongType
from dependencies.spark import start_spark

JOB_NAME = "gw_csv_formatter"
SEASON = "2022-23"
OUTPUT_PATH = f"C:/repos/sports-data-processor/data/football/fpl-ingest/players/gws/season={SEASON}/"


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
    gw_csv_df = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ",")
        .load("C:/repos/sports-data-processor/data/merged_gw.csv")
        .drop("name", "position", "team", "xP", "season")
    )

    teams_df = (
        spark
        .read
        .format("parquet")
        .load(f"C:/repos/sports-data-processor/data/football/fpl-ingest/teams/season={SEASON}")
        .withColumnRenamed("name", "opponent_team")
        .select("opponent_team", "id")
    )

    return gw_csv_df, teams_df


def transform_data(gw_csv_df, teams_df):
    """
    Transform json data into a DataFrame.
    """
    joined_df = (
        gw_csv_df
        .join(teams_df,
              on=["opponent_team"],
              how="inner")
        .drop("opponent_team")
        .withColumnRenamed("id", "opponent_team")
        .select("element", "fixture", "opponent_team", "total_points", "was_home", "kickoff_time", "team_h_score",
                "team_a_score", "round", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded",
                "own_goals", "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "bps",
                "influence", "creativity", "threat", "ict_index", "starts", "expected_goals", "expected_assists",
                "expected_goal_involvements", "expected_goals_conceded", "value", "transfers_balance", "selected",
                "transfers_in", "transfers_out")
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
        .partitionBy("round")
        .mode("overwrite")
        .save(OUTPUT_PATH)
    )


if __name__ == '__main__':
    main()
