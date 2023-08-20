from dependencies.spark import start_spark


def main():
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=[])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    # load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    fpl_players_df = (
        spark
        .read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ",")
        .load("C:/repos/fpl-points-predictor/data/football/raw-ingress/fpl/players/gws/")
        .select("name", "position", "team", "assists", "bonus", "goals_conceded", "goals_scored", "kickoff_time",
                "minutes", "round", "saves", "starts", "value", "was_home", "yellow_cards", "opponent_team")
    )

    return fpl_players_df


def transform_data(df):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df.show()


#
#
# def load_data(df):
#     """Collect data locally and write to CSV.
#
#     :param df: DataFrame to print.
#     :return: None
#     """
#     (df
#      .coalesce(1)
#      .write
#      .csv('loaded_data', mode='overwrite', header=True))
#     return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
