import json
import requests
import pyspark.sql.functions as fn
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    BooleanType,
    TimestampType,
)
from dependencies.spark import start_spark

JOB_NAME = "fpl_gw_ingest"
SEASON = "2023-24"
HISTORY_ENDPOINT = "https://fantasy.premierleague.com/api/element-summary/"
OUTPUT_PATH = f"C:/sports-data-processor/football/fpl-ingest/players/gws/season={SEASON}"

HISTORY_SCHEMA = StructType(
    [
        StructField("element", IntegerType(), True),
        StructField("fixture", IntegerType(), True),
        StructField("opponent_team", IntegerType(), True),
        StructField("total_points", IntegerType(), True),
        StructField("was_home", BooleanType(), True),
        StructField("kickoff_time", StringType(), True),
        StructField("team_h_score", IntegerType(), True),
        StructField("team_a_score", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("minutes", IntegerType(), True),
        StructField("goals_scored", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("clean_sheets", IntegerType(), True),
        StructField("goals_conceded", IntegerType(), True),
        StructField("own_goals", IntegerType(), True),
        StructField("penalties_saved", IntegerType(), True),
        StructField("penalties_missed", IntegerType(), True),
        StructField("yellow_cards", IntegerType(), True),
        StructField("red_cards", IntegerType(), True),
        StructField("saves", IntegerType(), True),
        StructField("bonus", IntegerType(), True),
        StructField("bps", IntegerType(), True),
        StructField("influence", StringType(), True),
        StructField("creativity", StringType(), True),
        StructField("threat", StringType(), True),
        StructField("ict_index", StringType(), True),
        StructField("starts", IntegerType(), True),
        StructField("expected_goals", StringType(), True),
        StructField("expected_assists", StringType(), True),
        StructField("expected_goal_involvements", StringType(), True),
        StructField("expected_goals_conceded", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("transfers_balance", IntegerType(), True),
        StructField("selected", IntegerType(), True),
        StructField("transfers_in", IntegerType(), True),
        StructField("transfers_out", IntegerType(), True),
    ]
)


def run():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        gw_raw_data = extract_data(spark)
        gw_df = transform_data(gw_raw_data, spark)
        load_data(gw_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets player gameweek data from FPL API, using player ids from elements data.
    """

    # Elements data, which contains column of player ids
    elements_df = (
        spark.read.format("parquet")
        .load(
            f"C:/sports-data-processor/football/fpl-ingest/players/elements/season={SEASON}"
        )
        .select("id")
    )

    id_list = elements_df.select(fn.collect_list("id")).first()[0]

    # Temp dictionary to hold gameweek data for each player
    gw_data = {"history": []}

    for element_id in id_list:
        current_gw_response = requests.get(f"{HISTORY_ENDPOINT}{element_id}")
        current_gw_response.raise_for_status()
        gw_data["history"].extend(json.loads(current_gw_response.text)["history"])

    return gw_data["history"]


def transform_data(gw_data, spark):
    """
    Transform gameweek data into a DataFrame.
    """
    gw_df = spark.createDataFrame(gw_data, HISTORY_SCHEMA).withColumn(
        "kickoff_time",
        fn.from_utc_timestamp(fn.col("kickoff_time"), "UTC").cast(TimestampType()),
    )
    return gw_df


def load_data(gw_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        gw_df.coalesce(1)
        .write.format("parquet")
        .partitionBy("round")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


if __name__ == "__main__":
    run()
