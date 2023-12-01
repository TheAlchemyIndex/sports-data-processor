# from jobs.averages import teams_averages, players_averages, season_averages
# from jobs.averages.back_fills import (
#     teams_averages_back_fill,
#     players_averages_back_fill,
#     season_averages_back_fill,
# )
# from jobs.predictions import fpl_points_predictor
# from jobs.predictions.back_fills import fpl_points_predictor_back_fill
from jobs.processing.fpl.players import fpl_players_stats_process


def main():
    # Processing data
    # fpl_fixtures_process.run()
    # fpl_team_stats_process.run()
    # fpl_current_players_attributes_process.run()
    fpl_players_stats_process.run()
    # player_name_validator.run()

    # Averages
    # teams_averages.run()
    # players_averages.run()
    # season_averages.run()

    # Predictions
    # fpl_points_predictor.run()

    # Back fills
    # teams_averages_back_fill.run()
    # players_averages_back_fill.run()
    # season_averages_back_fill.run()
    # fpl_points_predictor_back_fill.run()


if __name__ == "__main__":
    main()
