from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_players_attributes_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_attributes_output"
)
_fpl_ingest_path = ConfigurationParser.get_config("file_paths", "fpl_ingest_output")
_fpl_elements_path = ConfigurationParser.get_config("file_paths", "fpl_elements_output")
_fpl_teams_path = ConfigurationParser.get_config("file_paths", "fpl_teams_output")


def run():
    job_name = "fpl_current_players_attributes_process"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        elements_ingest_df, teams_ingest_df = extract_data(spark)
        players_attributes_df = transform_data(elements_ingest_df, teams_ingest_df)
        load_data(players_attributes_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets elements and teams ingest data.
    """
    elements_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_elements_path}/season={_season}")
        .select(
            "id",
            "first_name",
            "second_name",
            "chance_of_playing_next_round",
            "element_type",
            "team",
        )
    )

    teams_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_teams_path}/season={_season}")
        .select(
            "id",
            "name",
        )
        .withColumnRenamed("id", "team_id")
        .withColumnRenamed("name", "team_name")
    )

    return elements_df, teams_df


def transform_data(elements_df, teams_df):
    """
    Transform elements ingest data.
    """
    players_attributes_df = (
        elements_df.withColumn(
            "name", fn.concat_ws(" ", fn.col("first_name"), fn.col("second_name"))
        )
        .withColumn(
            "position",
            fn.when(fn.col("element_type") == 1, "GK")
            .when(fn.col("element_type") == 2, "DEF")
            .when(fn.col("element_type") == 3, "MID")
            .when(fn.col("element_type") == 4, "FWD"),
        )
        .withColumnRenamed("team", "team_id")
        .select("id", "name", "chance_of_playing_next_round", "position", "team_id")
    )

    player_attributes_with_teams_df = (
        players_attributes_df.join(teams_df, on=["team_id"], how="inner")
        .drop("team_id")
        .withColumnRenamed("team_name", "team")
    )

    return player_attributes_with_teams_df


def load_data(players_attributes_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_attributes_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/{_processed_data_output_path}/{_processed_players_attributes_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
