import json
import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType, BooleanType
from dependencies.spark import start_spark

JOB_NAME = "fpl_elements_ingest"
SEASON = "2023-24"
ELEMENTS_ENDPOINT = "https://fantasy.premierleague.com/api/bootstrap-static/"
OUTPUT_PATH = f"C:/repos/sports-data-processor/data/football/fpl-ingest/players/elements/season={SEASON}"

ELEMENTS_SCHEMA = StructType([
    StructField("chance_of_playing_next_round", IntegerType(), True),
    StructField("chance_of_playing_this_round", IntegerType(), True),
    StructField("code", LongType(), True),
    StructField("cost_change_event", IntegerType(), True),
    StructField("cost_change_event_fall", IntegerType(), True),
    StructField("cost_change_start", IntegerType(), True),
    StructField("cost_change_start_fall", IntegerType(), True),
    StructField("dreamteam_count", IntegerType(), True),
    StructField("element_type", IntegerType(), True),
    StructField("ep_next", StringType(), True),
    StructField("ep_this", StringType(), True),
    StructField("event_points", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("form", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("in_dreamteam", BooleanType(), True),
    StructField("news", StringType(), True),
    StructField("news_added", StringType(), True),
    StructField("now_cost", IntegerType(), True),
    StructField("photo", StringType(), True),
    StructField("points_per_game", StringType(), True),
    StructField("second_name", StringType(), True),
    StructField("selected_by_percent", StringType(), True),
    StructField("special", BooleanType(), True),
    StructField("squad_number", StringType(), True),
    StructField("status", StringType(), True),
    StructField("team", IntegerType(), True),
    StructField("team_code", IntegerType(), True),
    StructField("total_points", IntegerType(), True),
    StructField("transfers_in", LongType(), True),
    StructField("transfers_in_event", LongType(), True),
    StructField("transfers_out", LongType(), True),
    StructField("transfers_out_event", LongType(), True),
    StructField("value_form", StringType(), True),
    StructField("value_season", StringType(), True),
    StructField("web_name", StringType(), True),
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
    StructField("influence_rank", IntegerType(), True),
    StructField("influence_rank_type", IntegerType(), True),
    StructField("creativity_rank", IntegerType(), True),
    StructField("creativity_rank_type", IntegerType(), True),
    StructField("threat_rank", IntegerType(), True),
    StructField("threat_rank_type", IntegerType(), True),
    StructField("ict_index_rank", IntegerType(), True),
    StructField("ict_index_rank_type", IntegerType(), True),
    StructField("corners_and_indirect_freekicks_order", IntegerType(), True),
    StructField("corners_and_indirect_freekicks_text", StringType(), True),
    StructField("direct_freekicks_order", IntegerType(), True),
    StructField("direct_freekicks_text", StringType(), True),
    StructField("penalties_order", IntegerType(), True),
    StructField("penalties_text", StringType(), True),
    StructField("expected_goals_per_90", DoubleType(), True),
    StructField("saves_per_90", DoubleType(), True),
    StructField("expected_assists_per_90", DoubleType(), True),
    StructField("expected_goal_involvements_per_90", DoubleType(), True),
    StructField("expected_goals_conceded_per_90", DoubleType(), True),
    StructField("goals_conceded_per_90", DoubleType(), True),
    StructField("now_cost_rank", IntegerType(), True),
    StructField("now_cost_rank_type", IntegerType(), True),
    StructField("form_rank", IntegerType(), True),
    StructField("form_rank_type", IntegerType(), True),
    StructField("points_per_game_rank", IntegerType(), True),
    StructField("points_per_game_rank_type", IntegerType(), True),
    StructField("selected_rank", IntegerType(), True),
    StructField("selected_rank_type", IntegerType(), True),
    StructField("starts_per_90", DoubleType(), True),
    StructField("clean_sheets_per_90", DoubleType(), True)
])


def main():
    spark, log, config = start_spark(
        app_name=JOB_NAME,
        files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        elements_data = extract_data()
        elements_df = transform_data(elements_data, spark)
        load_data(elements_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data():
    """
    Extract data from API.
    """
    response = requests.get(ELEMENTS_ENDPOINT)
    response.raise_for_status()  # Raise an error for non-200 status codes
    elements_data = json.loads(response.text)["elements"]

    for player in elements_data:
        for field in [
            "expected_goals_per_90", "expected_assists_per_90",
            "expected_goal_involvements_per_90", "expected_goals_conceded_per_90",
            "saves_per_90", "goals_conceded_per_90",
            "starts_per_90", "clean_sheets_per_90"
        ]:
            player[field] = float(player[field])

    return elements_data


def transform_data(elements_data, spark):
    """
    Transform json data into a DataFrame.
    """
    elements_df = spark.createDataFrame(elements_data, ELEMENTS_SCHEMA)
    return elements_df


def load_data(elements_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        elements_df
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(OUTPUT_PATH)
    )


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
