from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark
from jobs.averages.average_calculator import (
    last_n_rows,
    calculate_partitioned_avg,
    last_value_in_col,
)

_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_players_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_stats_output"
)
_players_average_output_path = ConfigurationParser.get_config(
    "file_paths", "players_average_output"
)


def run():
    job_name = "players_averages"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        players_df = extract_data(spark)
        last_five_rows_avg_df = transform_data(players_df)
        load_data(last_five_rows_avg_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets processed players data.
    """
    players_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_processed_data_path}/{_processed_players_path}")
        .withColumnRenamed("kickoff_time", "date")
        .filter(fn.col("minutes") > 0)
    )

    return players_df


def transform_data(players_df):
    """
    Transform processed players data.
    """
    players_df_recent_price = last_value_in_col(players_df, "name", "value", "price")

    players_df_recent_position = last_value_in_col(
        players_df_recent_price, "name", "position", "position"
    )

    last_five_rows_df = last_n_rows(players_df_recent_position, "was_home", "name", 5)

    last_five_rows_avg_df = (
        last_five_rows_df.withColumn(
            "goals_scored_avg",
            calculate_partitioned_avg("was_home", "name", "goals_scored"),
        )
        .withColumn(
            "assists_avg", calculate_partitioned_avg("was_home", "name", "assists")
        )
        .withColumn("bonus_avg", calculate_partitioned_avg("was_home", "name", "bonus"))
        .withColumn(
            "goals_conceded_avg",
            calculate_partitioned_avg("was_home", "name", "goals_conceded"),
        )
        .withColumn(
            "yellow_cards_avg",
            calculate_partitioned_avg("was_home", "name", "yellow_cards"),
        )
        .withColumn(
            "minutes_avg_last_5",
            calculate_partitioned_avg("was_home", "name", "minutes"),
        )
        .withColumn("saves_avg", calculate_partitioned_avg("was_home", "name", "saves"))
        .select(
            "name",
            "team",
            "position",
            "was_home",
            "price",
            "goals_scored_avg",
            "assists_avg",
            "bonus_avg",
            "goals_conceded_avg",
            "yellow_cards_avg",
            "minutes_avg_last_5",
            "saves_avg",
        )
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
        .save(f"{_bucket}/{_players_average_output_path}")
    )


if __name__ == "__main__":
    run()
