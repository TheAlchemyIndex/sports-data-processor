from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")

team_name_mapping = {
    "Luton Town": "Luton",
    "Nottingham Forest": "Nott'm Forest",
    "Sheffield United": "Sheffield Utd",
}

promoted_teams = {
    "2021-22": ["Bournemouth", "Fulham", "Nottingham Forest"],
    "2022-23": ["Burnley", "Luton Town", "Sheffield United"],
}


def get_previous_season():
    split_season = _season.split("-")
    previous_season_start = int(split_season[0]) - 1
    previous_season_end = int(split_season[1]) - 1
    return str(f"{previous_season_start}-{previous_season_end}")


def run():
    job_name = "fixture_downloader_promoted_teams_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        promoted_teams_df = extract_data(spark)
        processed_promoted_teams_df = transform_data(promoted_teams_df)
        load_data(processed_promoted_teams_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Loads promoted teams data.
    """
    fixtures_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fixture-downloader/")
        .filter(fn.col("season") == get_previous_season())
        .filter(fn.col("league") == "Championship")
    )

    return fixtures_df


def transform_data(fixtures_df):
    """
    Transforms fixtures data.
    """
    teams_filter = promoted_teams.get(get_previous_season())

    processed_promoted_teams_df = (
        fixtures_df.withColumn(
            "date", fn.to_date(fn.split(fn.col("DateUtc"), " ").getItem(0))
        )
        .withColumn(
            "goals_scored",
            fn.when(
                fn.col("HomeTeam").isin(teams_filter), fn.col("HomeTeamScore")
            ).otherwise(fn.col("AwayTeamScore")),
        )
        .withColumn(
            "goals_conceded",
            fn.when(
                fn.col("HomeTeam").isin(teams_filter), fn.col("AwayTeamScore")
            ).otherwise(fn.col("HomeTeamScore")),
        )
        .withColumn(
            "team_type",
            fn.when(fn.col("HomeTeam").isin(teams_filter), fn.lit("h")).otherwise(
                fn.lit("a")
            ),
        )
        .withColumnRenamed("RoundNumber", "event")
        .withColumn(
            "team",
            fn.when(
                fn.col("HomeTeam").isin(teams_filter), fn.col("HomeTeam")
            ).otherwise(fn.col("AwayTeam")),
        )
        .filter(fn.col("team").isin(teams_filter))
        .replace(to_replace=team_name_mapping, subset="team")
        .dropDuplicates()
        .select("event", "date", "goals_scored", "goals_conceded", "team_type", "team")
    )

    return processed_promoted_teams_df


def load_data(processed_fixtures_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        processed_fixtures_df.write.format("parquet")
        .partitionBy("team")
        .mode("append")
        .save(
            f"{_bucket}/processed-ingress/teams/season={get_previous_season()}/source=fixture_downloader/"
        )
    )


if __name__ == "__main__":
    run()
