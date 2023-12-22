from jobs.averages import season_averages
from jobs.averages.back_fills import teams_averages_back_fill, players_averages_back_fill
from jobs.predictions.back_fills import fpl_points_predictor_back_fill
from jobs.processing.fpl.players.back_fills import fpl_players_stats_process_back_fill


def run(season, starting_gw, ending_gw):
    for gw in range(starting_gw, ending_gw + 1):
        # fpl_players_stats_process_back_fill.run(season, gw)
        teams_averages_back_fill.run(season, gw)
        players_averages_back_fill.run(season, gw)
        season_averages.run()
        fpl_points_predictor_back_fill.run(season, gw)
