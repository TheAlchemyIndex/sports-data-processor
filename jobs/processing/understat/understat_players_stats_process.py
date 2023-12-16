from pyspark.sql import functions as fn, Window

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_season = ConfigurationParser.get_config("external", "season")
_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")

team_name_mapping = {
    "Almeria": "Almería",
    "Atletico Madrid": "Atlético Madrid",
    "Borussia M.Gladbach": "Borussia Mönchengladbach",
    "Cadiz": "Cádiz",
    "FC Cologne": "FC Köln",
    "Manchester City": "Man City",
    "Manchester United": "Man Utd",
    "Milan": "AC Milan",
    "Newcastle United": "New United",
    "Nottingham Forest": "Nott'm Forest",
    "Paris Saint Germain": "Paris Saint-Germain",
    "RasenBallsport Leipzig": "RB Leipzig",
    "Sheffield United": "Sheffield",
    "Tottenham": "Spurs",
    "Wolverhampton Wanderers": "Wolves",
}

player_name_mapping = {
    "Aaron Ramsey": "Aaron James Ramsey",
    "André Gomes": "André Tavares Gomes",
    "Anssumane Fati": "Anssumane Fati Vieira",
    "Chimuanya Ugochukwu": "Lesley Ugochukwu",
    "Diego Carlos": "Diego Carlos Santos Silva",
    "Emerson": "Emerson Palmieri dos Santos",
    "Fode Toure": "Fodé Ballo-Touré",
    "João Félix": "João Félix Sequeira",
    "Igor Julio": "Igor Julio dos Santos de Paulo",
    "Josko Gvardiol": "Joško Gvardiol",
    "Iyenoma Destiny Udogie": "Destiny Udogie",
    "Nuno Tavares": "Nuno Varela Tavares",
    "Ola Aina": "Olu Aina",
}


def get_previous_season():
    split_season = _season.split("-")
    previous_season_start = int(split_season[0]) - 1
    previous_season_end = int(split_season[1]) - 1
    return str(f"{previous_season_start}-{previous_season_end}")


def run():
    job_name = "understat_players_stats_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        understat_players_df, missing_players_df, fixture_downloader_df = extract_data(
            spark
        )
        processed_understat_df = transform_data(
            understat_players_df, missing_players_df, fixture_downloader_df
        )
        load_data(processed_understat_df)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark):
    """
    Gets understat players raw ingress data.
    """
    understat_players_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/understat/players/")
        .filter(fn.col("season") == get_previous_season())
        .filter(fn.col("league") != "EPL")
    ).drop("id", "position")

    missing_players_df = (
        spark.read.format("parquet")
        .load(f"{_bucket}/processed-ingress/players/missing-players/")
        .filter(fn.col("season") == _season)
        .select("id", "name", "position")
    )

    fixture_downloader_df = (
        spark.read.format("parquet")
        .load(
            f"{_bucket}/processed-ingress/fixtures/season={get_previous_season()}/fixture-downloader/"
        )
        .select("event", "kickoff_time", "team_h", "team_a")
    )

    return understat_players_df, missing_players_df, fixture_downloader_df


def transform_data(understat_players_df, missing_players_df, fixture_downloader_df):
    away_fixtures_df = (
        fixture_downloader_df.withColumnRenamed("team_a", "team_h_temp")
        .withColumnRenamed("team_h", "team_a")
        .withColumnRenamed("team_h_temp", "team_h")
    )

    processed_fixtures_df = (
        fixture_downloader_df.unionByName(away_fixtures_df)
        .withColumn("date", fn.split(fn.col("kickoff_time"), " ").getItem(0))
        .withColumn("kickoff_time", fn.regexp_replace("kickoff_time", " ", "T"))
        .withColumnRenamed("team_h", "team")
        .withColumnRenamed("event", "round")
        .drop("team_a")
    )

    # Gets the player's team by getting the most frequent value in the h_team column
    window = Window.partitionBy("name", "h_team").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    filtered_understat_players_df = understat_players_df.replace(
        to_replace=player_name_mapping, subset="name"
    ).join(
        missing_players_df,
        on="name",
        how="inner",
    )

    processed_understat_players_df = (
        (
            filtered_understat_players_df.replace(
                to_replace=team_name_mapping, subset="h_team"
            )
            .replace(to_replace=team_name_mapping, subset="a_team")
            # For working out if match was home or away
            .withColumn("team_count", fn.count("h_team").over(window))
            .withColumn("total_points", fn.lit(None).cast("long"))
            .withColumn(
                "was_home", fn.when(fn.col("team_count") != 1, True).otherwise(False)
            )
            .withColumn(
                "goals_conceded",
                fn.when(fn.col("was_home"), fn.col("a_goals")).otherwise(
                    fn.col("h_goals")
                ),
            )
            .withColumn(
                "clean_sheets",
                fn.when(
                    (fn.col("time") >= 60) & (fn.col("goals_conceded") == 0), 1
                ).otherwise(0),
            )
            .withColumn("yellow_cards", fn.lit(None).cast("long"))
            .withColumn("saves", fn.lit(None).cast("long"))
            .withColumn("bonus", fn.lit(None).cast("long"))
            .withColumn("value", fn.lit(None).cast("double"))
            .withColumn(
                "opponent_team",
                fn.when(fn.col("was_home"), fn.col("a_team")).otherwise(
                    fn.col("h_team")
                ),
            )
            .withColumn("first_name", fn.split(fn.col("name"), " ").getItem(0))
            .withColumn(
                "second_name",
                fn.expr(
                    "concat_ws(' ', slice(split(name, ' '), 2, size(split(name, ' '))))"
                ),
            )
            .withColumn("chance_of_playing_next_round", fn.lit(None).cast("long"))
            .withColumn("news", fn.lit(None).cast("string"))
            .withColumn(
                "team",
                fn.when(fn.col("team_count") != 1, fn.col("h_team")).otherwise(
                    fn.col("a_team")
                ),
            )
            # Fixing incorrect data for Jean-Ricner Bellegarde
            .withColumn(
                "date",
                fn.when(
                    (fn.col("name") == "Jean-Ricner Bellegarde")
                    & (fn.col("date") == "2023-04-30"),
                    fn.lit("2023-04-28"),
                ).otherwise(fn.col("date")),
            )
            .drop("kickoff_time")
            # TODO Do this better - Bryan Gil has Spurs in his data for some reason
            .filter(fn.col("team") != "Spurs")
        )
        .join(processed_fixtures_df, on=["date", "team"], how="inner")
        .select(
            "id",
            "total_points",
            "was_home",
            "kickoff_time",
            fn.col("time").alias("minutes"),
            fn.col("goals").alias("goals_scored"),
            "assists",
            "clean_sheets",
            "goals_conceded",
            "yellow_cards",
            "saves",
            "bonus",
            "value",
            "opponent_team",
            "first_name",
            "second_name",
            "chance_of_playing_next_round",
            "news",
            "position",
            "team",
            "name",
            "round",
        )
    )

    return processed_understat_players_df


def load_data(processed_understat_players_df):
    """
    Write DataFrame as Parquet format.
    """
    (
        processed_understat_players_df.write.format("parquet")
        .partitionBy("name", "round")
        .mode("overwrite")
        .save(
            f"{_bucket}/processed-ingress/players/stats/season={get_previous_season()}/source=understat/"
        )
    )
