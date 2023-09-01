from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_processed_data_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_data_output"
)
_processed_players_names_output_path = ConfigurationParser.get_config(
    "file_paths", "processed_players_names_output"
)
_fpl_ingest_path = ConfigurationParser.get_config("file_paths", "fpl_ingest_output")
_fpl_elements_path = ConfigurationParser.get_config("file_paths", "fpl_elements_output")


def run():
    job_name = "fpl_current_players_names_process"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        elements_ingest_df = extract_data(spark)
        players_names_df = transform_data(elements_ingest_df)
        load_data(players_names_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets elements ingest data.
    """
    elements_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/{_fpl_ingest_path}/{_fpl_elements_path}/season={_season}")
        .select("id", "first_name", "second_name", "chance_of_playing_next_round")
    )

    return elements_df


def transform_data(elements_df):
    """
    Transform elements ingest data.
    """
    players_names_df = elements_df.withColumn(
        "name", fn.concat_ws(" ", fn.col("first_name"), fn.col("second_name"))
    ).select("id", "name", "chance_of_playing_next_round")

    return players_names_df


def load_data(players_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_names_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/{_processed_data_output_path}/{_processed_players_names_output_path}/season={_season}"
        )
    )


if __name__ == "__main__":
    run()
