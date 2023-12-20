from jobs.averages import players_averages, teams_averages, season_averages
from jobs.averages.back_fills import teams_averages_back_fill, players_averages_back_fill, master_average_back_fill
from jobs.predictions import fpl_points_predictor
from jobs.processing.fpl.fixtures import fpl_fixtures_process
from jobs.processing.fpl.players import (
    fpl_players_stats_process,
    fpl_current_players_attributes_process,
)
from jobs.processing.fpl.teams import fpl_team_stats_process
from jobs.processing.understat import understat_players_stats_process
from jobs.validation import missing_players


def main():
    # Processing data
    # fpl_fixtures_process.run()
    # fpl_team_stats_process.run()
    # fpl_current_players_attributes_process.run()
    # fpl_players_stats_process.run()

    # Averages
    # teams_averages.run()
    # players_averages.run()
    # season_averages.run()

    # Predictions
    # fpl_points_predictor.run()

    # Back fills
    master_average_back_fill.run("2023-24", 12, 12)
    # fpl_points_predictor_back_fill.run()


if __name__ == "__main__":
    main()
