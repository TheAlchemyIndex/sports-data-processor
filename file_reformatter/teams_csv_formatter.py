from dependencies.spark import start_spark

JOB_NAME = "teams_csv_formatter"
SEASON = "2022-23"
OUTPUT_PATH = f"C:/repos/fpl-points-predictor/data/football/fpl-ingest/teams/season={SEASON}/"


def main():
    spark, log, config = start_spark(
        app_name=JOB_NAME,
        files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        teams_csv_data = extract_data(spark)
        load_data(teams_csv_data)
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
        .load("C:/repos/fpl-points-predictor/data/teams.csv")
        .select("code", "draw", "form", "id", "loss", "name", "played", "points", "position", "short_name", "strength",
                "team_division", "unavailable", "win", "strength_overall_home", "strength_overall_away",
                "strength_attack_home", "strength_attack_away", "strength_defence_home", "strength_defence_away",
                "pulse_id")
    )

    return gw_csv_df


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
