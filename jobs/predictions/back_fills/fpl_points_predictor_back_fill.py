import json

import requests
from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import start_spark

# _season = ConfigurationParser.get_config("external", "season")
_season = "2022-23"
_bucket = ConfigurationParser.get_config("file_paths", "football_bucket")
_season_averages_path = ConfigurationParser.get_config(
    "file_paths", "season_averages_output"
)
_predictions_output_path = ConfigurationParser.get_config(
    "file_paths", "predictions_data_output"
)
_fpl_predicted_points_output_path = ConfigurationParser.get_config(
    "file_paths", "fpl_predicted_points_output"
)


def run():
    job_name = "fpl_points_predictor"

    spark, log = start_spark(app_name=job_name, files=[])
    log.warn(f"{job_name} running.")

    try:
        season_averages_df = extract_data(spark)
        predicted_points_df = transform_data(season_averages_df)
        load_data(predicted_points_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets averages for players for the full season.
    """
    season_averages_df = spark.read.format("parquet").load(
        f"{_bucket}/{_season_averages_path}"
    )

    return season_averages_df


def transform_data(season_averages_df):
    """
    Calculates predicted points.
    """
    expected_points_df = (
        season_averages_df.na.fill(value=1, subset=["opponent_goals_scored_avg"])
        .na.fill(value=1, subset=["opponent_goals_conceded_avg"])
        .withColumn(
            "minute_points",
            fn.when(fn.col("minutes_avg_last_5") >= 60, 2)
            .when(fn.col("minutes_avg_last_5") > 0, 1)
            .otherwise(0),
            )
        .withColumn(
            "goal_points",
            fn.when(
                fn.col("position") == "FWD",
                fn.round(
                    fn.col("goals_scored_avg")
                    * fn.col("opponent_goals_conceded_avg")
                    * fn.lit(4),
                    2,
                    ),
                )
            .when(
                fn.col("position") == "MID",
                fn.round(
                    fn.col("goals_scored_avg")
                    * fn.col("opponent_goals_conceded_avg")
                    * fn.lit(5),
                    2,
                    ),
                )
            .when(
                fn.col("position") == "DEF",
                fn.round(
                    fn.col("goals_scored_avg")
                    * fn.col("opponent_goals_conceded_avg")
                    * fn.lit(6),
                    2,
                    ),
                )
            .otherwise(0),
            )
        .withColumn(
            "clean_sheet_points",
            fn.when(
                (fn.col("position") == "GK") | (fn.col("position") == "DEF"),
                fn.when(
                    (
                            fn.col("goals_conceded_avg")
                            * fn.col("opponent_goals_scored_avg")
                            < 1
                    )
                    & (fn.col("minutes_avg_last_5") >= 60),
                    fn.round(
                        (
                                4
                                - (
                                        fn.col("goals_conceded_avg")
                                        * fn.col("opponent_goals_scored_avg")
                                )
                        ),
                        2,
                    ),
                    )
                .when(
                    (fn.col("goals_conceded_avg") * fn.col("opponent_goals_scored_avg"))
                    > 1,
                    fn.round(
                        (
                                0
                                - (
                                        fn.col("goals_conceded_avg")
                                        * fn.col("opponent_goals_scored_avg")
                                )
                                / 2
                        ),
                        2,
                    ),
                    )
                .otherwise(0),
                ).otherwise(
                fn.when(
                    fn.col("position") == "MID",
                    fn.when(
                        (
                                fn.col("goals_conceded_avg")
                                * fn.col("opponent_goals_scored_avg")
                                < 1
                        )
                        & (fn.col("minutes_avg_last_5") >= 60),
                        fn.round(
                            (
                                    1
                                    - (
                                            fn.col("goals_conceded_avg")
                                            * fn.col("opponent_goals_scored_avg")
                                    )
                            ),
                            2,
                        ),
                        ).otherwise(0),
                    ).otherwise(0)
            ),
        )
        .withColumn("assist_points", fn.round(fn.col("assists_avg") * 3, 2))
        .withColumn("save_points", fn.round(fn.col("saves_avg") / 3, 2))
        .withColumn(
            "expected_points",
            fn.round(
                fn.col("minute_points")
                + fn.col("goal_points")
                + fn.col("clean_sheet_points")
                + fn.col("assist_points")
                + fn.col("save_points")
                + fn.col("bonus_avg")
                - fn.col("yellow_cards_avg"),
                2,
                ),
        )
        .select(
            "date",
            "event",
            "id",
            "name",
            "team",
            "team_type",
            "opponent",
            "opponent_type",
            "position",
            "expected_points",
            "minutes_avg_last_5",
            "chance_of_playing_next_round"
        )
    )

    return expected_points_df


def load_data(predicted_points_df):
    """
    Write DataFrame as Parquet format.
    """
    # TODO Work out a better way to get next gameweek that can be used across other jobs
    # events_response = requests.get(
    #     "https://fantasy.premierleague.com/api/bootstrap-static/"
    # )
    # events_response.raise_for_status()
    # events_data = json.loads(events_response.text)["events"]
    # gw_num = 0
    # for event in events_data:
    #     if event["is_next"]:
    #         gw_num = event["id"]

    gw_num = 38

    (
        predicted_points_df.filter(fn.col("event") == gw_num)
        .coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/{_predictions_output_path}/{_fpl_predicted_points_output_path}/season={_season}/round={gw_num}"
        )
    )


if __name__ == "__main__":
    run()
