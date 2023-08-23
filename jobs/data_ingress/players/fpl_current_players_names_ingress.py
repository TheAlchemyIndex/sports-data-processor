from pyspark.sql import functions as fn

from dependencies.spark import start_spark

JOB_NAME = "fpl_current_players_names_ingress"
SEASON = "2023-24"
OUTPUT_PATH = f"C:/repos/sports-data-processor/data/football/raw-ingress/players/names/season={SEASON}"


def main():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        elements_df = extract_data(spark)
        players_names_df = transform_data(elements_df)
        load_data(players_names_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets players data.
    """
    elements_df = (
        spark.read.format("parquet")
        .load(f"C:/repos/sports-data-processor/data/football/fpl-ingest/players/elements/season={SEASON}")
        .select("id", "first_name", "second_name")
    )

    return elements_df


def transform_data(elements_df):
    """
    Transform players data.
    """
    players_names_df = (
        elements_df
        .withColumn("name", fn.concat_ws(" ", fn.col("first_name"), fn.col("second_name")))
        .select("id", "name")
    )

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


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
