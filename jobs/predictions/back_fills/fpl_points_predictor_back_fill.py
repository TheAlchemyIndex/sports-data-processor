from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")


def run(season, gw):
    job_name = "fpl_points_predictor_back_fill"

    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        next_gw = gw + 1
        season_averages_df = extract_data(spark)
        predicted_points_df = transform_data(season_averages_df)
        load_data(predicted_points_df, season, next_gw)
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
        f"{_bucket}/averages/season/"
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
                fn.col("goals_scored_avg")
                * fn.col("opponent_goals_conceded_avg")
                * fn.lit(4),
            )
            .when(
                fn.col("position") == "MID",
                fn.col("goals_scored_avg")
                * fn.col("opponent_goals_conceded_avg")
                * fn.lit(5),
            )
            .when(
                fn.col("position") == "DEF",
                fn.col("goals_scored_avg")
                * fn.col("opponent_goals_conceded_avg")
                * fn.lit(6),
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
                    4
                    - (
                        fn.col("goals_conceded_avg")
                        * fn.col("opponent_goals_scored_avg")
                    ),
                )
                .when(
                    (fn.col("goals_conceded_avg") * fn.col("opponent_goals_scored_avg"))
                    > 1,
                    0
                    - (
                        fn.col("goals_conceded_avg")
                        * fn.col("opponent_goals_scored_avg")
                    )
                    / 2,
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
                        1
                        - (
                            fn.col("goals_conceded_avg")
                            * fn.col("opponent_goals_scored_avg")
                        ),
                    ).otherwise(0),
                ).otherwise(0)
            ),
        )
        .withColumn("assist_points", fn.col("assists_avg") * 3)
        .withColumn("save_points", fn.col("saves_avg") / 3)
        .withColumn(
            "expected_points",
            fn.when(fn.col("chance_of_playing_next_round") == 0, 0).otherwise(
                fn.round(
                    (
                        fn.col("minute_points")
                        + fn.col("goal_points")
                        + fn.col("clean_sheet_points")
                        + fn.col("assist_points")
                        + fn.col("save_points")
                        + fn.col("bonus_avg")
                        - fn.col("yellow_cards_avg")
                    )
                    * fn.col("minutes_percentage_played_last_5"),
                    2,
                ),
            ),
        )
        .withColumnRenamed("event", "round")
        .select(
            "date",
            "round",
            "id",
            "name",
            "price",
            "team",
            "team_type",
            "opponent",
            "opponent_type",
            "position",
            "expected_points",
            "minutes_avg_last_5",
            "chance_of_playing_next_round",
        )
    )

    return expected_points_df


def load_data(predicted_points_df, season, next_gw):
    """
    Write DataFrame as Parquet format.
    """
    (
        predicted_points_df.filter(fn.col("round") == next_gw)
        .coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .save(
            f"{_bucket}/predictions/fpl/predicted-points/season={season}/round={next_gw}"
        )
    )

    (
        predicted_points_df.filter(fn.col("round") > next_gw)
        .withColumnRenamed("round", "upcoming_round")
        .coalesce(1)
        .write.format("parquet")
        .partitionBy("upcoming_round")
        .mode("overwrite")
        .save(
            f"{_bucket}/predictions/fpl/season-predictions/season={_season}/round={next_gw}"
        )
    )
