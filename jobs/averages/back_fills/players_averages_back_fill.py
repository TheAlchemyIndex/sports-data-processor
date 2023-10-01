from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark
from jobs.averages.util.average_calculator import (
    last_n_rows,
    calculate_partitioned_avg,
    last_value_in_col,
    calculate_partitioned_avg_single,
)

_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_players_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_stats_output"
)
_players_averages_output_path = ConfigurationParser.get_config(
    "file_paths", "players_averages_output"
)
_processed_players_attributes_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_attributes_output"
)


def run():
    job_name = "players_averages"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        players_df, players_attributes_df = extract_data(spark)
        last_five_rows_avg_df = transform_data(players_df, players_attributes_df)
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
        .drop("id")
    )

    first_df = players_df.filter(fn.col("season") == "2021-22")

    # second_df = players_df.filter(fn.col("season") == "2022-23").filter(
    #     fn.col("round") < 38
    # )

    second_df = players_df.filter(fn.col("season") == "2022-23")

    third_df = players_df.filter(fn.col("season") == "2023-24").filter(
        fn.col("round") < 7
    )

    union_df = first_df.union(second_df)
    union_df_2 = union_df.union(third_df)

    players_attributes_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_processed_data_path}/{_processed_players_attributes_path}/")
        .filter(fn.col("season") == "2023-24")
        .filter(fn.col("round") == 6)
        .select("name", "id")
    )

    return union_df_2, players_attributes_df


def transform_data(players_df, players_attributes_df):
    """
    Transform processed players data.
    """
    current_players_df = players_attributes_df.join(
        players_df, on=["name"], how="inner"
    ).orderBy(fn.col("date").asc())

    players_df_recent_price = last_value_in_col(
        current_players_df, "name", "value", "price"
    )

    players_df_recent_position = last_value_in_col(
        players_df_recent_price, "name", "position", "position"
    )

    players_df_recent_team = last_value_in_col(
        players_df_recent_position, "name", "team", "team"
    )

    players_df_recent_id = last_value_in_col(players_df_recent_team, "name", "id", "id")

    players_df_min_percentage_player = players_df_recent_id.withColumn(
        "minutes_percentage_played_last_5",
        calculate_partitioned_avg_single("name", "minutes"),
    ).orderBy(fn.col("date").asc())

    players_df_min_percentage_player = players_df_min_percentage_player.withColumn(
        "minutes_percentage_played_last_5",
        fn.col("minutes_percentage_played_last_5") / 90,
    )

    players_df_min_percentage_player = last_value_in_col(
        players_df_min_percentage_player,
        "name",
        "minutes_percentage_played_last_5",
        "minutes_percentage_played_last_5",
    )

    players_df_recent_playing_chance = last_value_in_col(
        players_df_min_percentage_player,
        "name",
        "chance_of_playing_next_round",
        "chance_of_playing_next_round",
    ).filter(fn.col("minutes") > 0)

    players_df_avg_mins = players_df_recent_playing_chance.withColumn(
        "minutes_avg_last_5", calculate_partitioned_avg_single("name", "minutes")
    ).orderBy(fn.col("date").asc())

    players_df_recent_avg_mins = last_value_in_col(
        players_df_avg_mins, "name", "minutes_avg_last_5", "minutes_avg_last_5"
    )

    last_five_rows_df = last_n_rows(
        players_df_recent_avg_mins,
        "was_home",
        "name",
        5,
    )

    players_avg_df = (
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
        .withColumn("saves_avg", calculate_partitioned_avg("was_home", "name", "saves"))
        .select(
            "id",
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
            "chance_of_playing_next_round",
            "minutes_percentage_played_last_5",
        )
        .dropDuplicates()
    )

    return players_avg_df


def load_data(players_avg_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_avg_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{_bucket}/{_players_averages_output_path}")
    )