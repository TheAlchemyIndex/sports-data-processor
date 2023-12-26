from jobs import master_back_fill
from jobs.averages import teams_averages
from jobs.processing.fpl.fixtures import fpl_fixtures_process
from jobs.processing.fpl.players import fpl_current_players_attributes_process, fpl_players_stats_process
from jobs.processing.fpl.players.back_fills import fpl_players_stats_process_back_fill
from jobs.processing.fpl.teams import fpl_team_stats_process


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
    master_back_fill.run("2022-23", 0, 0)
    # fpl_points_predictor_back_fill.run()


if __name__ == "__main__":
    main()
