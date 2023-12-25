from pyspark.sql import functions as fn

from config import ConfigurationParser
from dependencies.spark import create_spark_session

_bucket = ConfigurationParser.get_config("file_paths", "sports-data-pipeline")

player_name_mapping = {
    "Adama Traoré Diarra": "Adama Traoré",
    "Alejandro Garnacho Ferreyra": "Alejandro Garnacho",
    "Alex Nicolao Telles": "Alex Telles",
    "André Filipe Tavares Gomes": "André Tavares Gomes",
    "Arnaut Danjuma": "Arnaut Danjuma Groeneveld",
    "Bamidele Alli": "Dele Alli",
    "Ben White": "Benjamin White",
    "Benjamin Chilwell": "Ben Chilwell",
    "Bernardo Mota Veiga de Carvalho e Silva": "Bernardo Veiga de Carvalho e Silva",
    "Bruno Miguel Borges Fernandes": "Bruno Borges Fernandes",
    "Cédric Soares": "Cédric Alves Soares",
    "David de Gea": "David De Gea Quintana",
    "Diogo Jota": "Diogo Teixeira da Silva",
    "Edward Nketiah": "Eddie Nketiah",
    "Emerson Aparecido Leite de Souza Junior": "Emerson Leite de Souza Junior",
    "Emiliano Martínez": "Emiliano Martínez Romero",
    "Fabio Silva": "Fábio Silva",
    "Gabriel Magalhães": "Gabriel dos Santos Magalhães",
    "Gabriel Teodoro Martinelli Silva": "Gabriel Martinelli Silva",
    "Hee-Chan Hwang": "Hwang Hee-chan",
    "Heung-Min Son": "Son Heung-min",
    "Javier Manquillo": "Javier Manquillo Gaitán",
    "Jeremy Sarmiento": "Jeremy Sarmiento Morante",
    "Joseph Gomez": "Joe Gomez",
    "Joseph Willock": "Joe Willock",
    "José Diogo Dalot Teixeira": "Diogo Dalot Teixeira",
    "João Pedro Cavaco Cancelo": "João Cancelo",
    "Luis Sinisterra Lucumí": "Luis Sinisterra",
    "Lyanco Evangelista Silveira Neves Vojnovic": "Lyanco Silveira Neves Vojnovic",
    "Marc Cucurella": "Marc Cucurella Saseta",
    "Mateo Kovacic": "Mateo Kovačić",
    "Matthew Cash": "Matty Cash",
    "Michale Olakigbe": "Michael Olakigbe",
    "Miguel Almirón": "Miguel Almirón Rejala",
    "Mohamed Naser El Sayed Elneny": "Mohamed Elneny",
    "Moisés Caicedo": "Moisés Caicedo Corozo",
    "Pablo Fornals": "Pablo Fornals Malla",
    "Pelenda Joshua Dasilva": "Josh Dasilva",
    "Rayan Ait Nouri": "Rayan Aït-Nouri",
    "Ricardo Domingos Barbosa Pereira": "Ricardo Barbosa Pereira",
    "Rúben Diogo da Silva Neves": "Rúben da Silva Neves",
    "Rúben Santos Gato Alves Dias": "Rúben Gato Alves Dias",
    "Sasa Lukic": "Saša Lukić",
    "Sergi Canós": "Sergi Canós Tenés",
    "Solomon March": "Solly March",
    "Tomas Soucek": "Tomáš Souček",
    "Vladimir Coufal": "Vladimír Coufal",
    "Yegor Yarmolyuk": "Yegor Yarmoliuk",
}


def run(season, gw):
    job_name = "fpl_current_players_attributes_process"
    spark, log = create_spark_session(app_name=job_name)
    log.warn(f"{job_name} running.")

    try:
        elements_ingest_df, teams_ingest_df = extract_data(spark, season, gw)
        players_attributes_df = transform_data(elements_ingest_df, teams_ingest_df)
        load_data(players_attributes_df, season, gw)
    except Exception as e:
        log.error(f"Error running {job_name}: {str(e)}")
    finally:
        log.warn(f"{job_name} is finished.")
        spark.stop()


def extract_data(spark, season, gw):
    """
    Gets elements and teams ingest data.
    """
    elements_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/players/elements/")
        .filter(fn.col("season") == season)
        .filter(fn.col("round") == gw)
        .select(
            "id",
            "first_name",
            "second_name",
            "chance_of_playing_next_round",
            "element_type",
            "team",
            "news",
            "now_cost",
        )
    )

    teams_df = (
        spark.read.format("json")
        .load(f"{_bucket}/raw-ingress/fpl/teams/")
        .filter(fn.col("season") == season)
        .select(
            "id",
            "name",
        )
        .withColumnRenamed("id", "team_id")
        .withColumnRenamed("name", "team_name")
    )

    return elements_df, teams_df


def transform_data(elements_df, teams_df):
    """
    Transform elements ingest data.
    """
    players_attributes_df = (
        elements_df.filter(~(elements_df.news.contains("loan")))
        .filter(~(elements_df.news.contains("Loan")))
        .filter(~(elements_df.news.contains("transfer")))
        .filter(~(elements_df.news.contains("Transfer")))
        .filter(~(elements_df.news.contains("left")))
        .filter(~(elements_df.news.contains("Left")))
        .filter(~(elements_df.news.contains("move")))
        .filter(~(elements_df.news.contains("Move")))
        .withColumn(
            "name", fn.concat_ws(" ", fn.col("first_name"), fn.col("second_name"))
        )
        .withColumn(
            "position",
            fn.when(fn.col("element_type") == 1, "GK")
            .when(fn.col("element_type") == 2, "DEF")
            .when(fn.col("element_type") == 3, "MID")
            .when(fn.col("element_type") == 4, "FWD"),
            )
        .withColumnRenamed("team", "team_id")
        .withColumnRenamed("now_cost", "price")
        .drop("element_type")
        .replace(to_replace=player_name_mapping, subset="name")
    )

    player_attributes_with_teams_df = (
        players_attributes_df.join(teams_df, on=["team_id"], how="inner")
        .drop("team_id")
        .withColumnRenamed("team_name", "team")
    )

    return player_attributes_with_teams_df


def load_data(players_attributes_df, season, gw):
    """
    Write DataFrame as Parquet format.
    """
    (
        players_attributes_df.coalesce(1)
        .write.format("parquet")
        .mode("append")
        .save(
            f"{_bucket}/processed-ingress/players/attributes/season={season}/round={gw}"
        )
    )
