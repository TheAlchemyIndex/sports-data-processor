from pyspark.sql import functions as fn

from dependencies.spark import start_spark

JOB_NAME = "fpl_current_players_names_process"
SEASON = "2023-24"
OUTPUT_PATH = f"C:/repos/sports-data-processor/data/football/processed-data/players/names/season={SEASON}"


def run():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        elements_ingest_df = extract_data(spark)
        players_names_df = transform_data(elements_ingest_df)
        load_data(players_names_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets elements ingest data.
    """
    elements_df = (
        spark.read.format("parquet")
        .load(
            f"C:/repos/sports-data-processor/data/football/fpl-ingest/players/elements/season={SEASON}"
        )
        .select("id", "first_name", "second_name")
    )

    return elements_df


def transform_data(elements_df):
    """
    Transform elements ingest data.
    """
    players_names_df = elements_df.withColumn(
        "name", fn.concat_ws(" ", fn.col("first_name"), fn.col("second_name"))
    ).select("id", "name")

    return players_names_df


def load_data(players_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_names_df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


if __name__ == "__main__":
    run()
