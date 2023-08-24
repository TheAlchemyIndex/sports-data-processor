from pyspark.sql import functions as fn

from dependencies.spark import start_spark

JOB_NAME = "fpl_player_stats_process"
OUTPUT_PATH = "C:/repos/sports-data-processor/data/football/processed-data/players/stats"


def main():
    spark, log, config = start_spark(app_name=JOB_NAME, files=[])
    log.warn(f"{JOB_NAME} running.")

    try:
        # Execute ETL pipeline
        gws_df, teams_df, players_names_df = extract_data(spark)
        player_stats_df = transform_data(gws_df, teams_df, players_names_df)
        load_data(player_stats_df)
    except Exception as e:
        log.error(f"Error running {JOB_NAME}: {str(e)}")
    finally:
        log.warn(f"{JOB_NAME} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets players data.
    """
    gws_df = (
        spark.read.format("parquet")
        .load("C:/repos/sports-data-processor/data/football/fpl-ingest/players/gws/")
        .withColumnRenamed("opponent_team", "opponent_id")
        .select("element", "opponent_id", "total_points", "was_home", "kickoff_time", "minutes", "goals_scored", "assists", "clean_sheets", "goals_conceded", "yellow_cards", "saves", "bonus", "value", "season", "round")
    )

    teams_df = (
        spark.read.format("parquet")
        .load("C:/repos/sports-data-processor/data/football/fpl-ingest/teams")
        .withColumnRenamed("id", "opponent_id")
        .withColumnRenamed("name", "opponent_team")
        .select("opponent_id", "opponent_team", "season")
    )

    players_names_df = (
        spark.read.format("parquet")
        .load("C:/repos/sports-data-processor/data/football/processed-data/players/names")
        .withColumnRenamed("id", "element")
    )

    return gws_df, teams_df, players_names_df


def transform_data(gws_df, teams_df, players_names_df):
    players_with_teams_df = (
        gws_df
        .join(teams_df,
              on=["opponent_id", "season"],
              how="inner"
              )
        .drop("opponent_id")
    )

    players_with_names_df = (
        players_with_teams_df
        .join(players_names_df,
              on=["element", "season"],
              how="inner"
              )
        .drop("element")
    )

    current_players_df = (
        players_names_df
        .filter(fn.col("season") == "2023-24")
        .select("name")
    )

    current_players_df = (
        current_players_df
        .join(players_with_names_df,
              on=["name"],
              how="inner"
              )
    )

    return current_players_df


def load_data(players_names_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_names_df
        .write.format("parquet")
        .partitionBy("season", "name", "round")
        .mode("overwrite")
        .save(f"{OUTPUT_PATH}")
    )


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
