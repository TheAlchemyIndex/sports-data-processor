from jobs.averages.back_fills import (
    teams_averages_back_fill,
    players_averages_back_fill, season_averages_back_fill,
)
from jobs.predictions.back_fills import fpl_points_predictor_back_fill
from jobs.processing.fpl.players.back_fills import fpl_players_stats_process_back_fill, \
    fpl_current_players_attributes_process_back_fill


def run(season, starting_gw, ending_gw):
    for gw in range(starting_gw, ending_gw + 1):
        if gw == 0:
            fpl_current_players_attributes_process_back_fill.run(season, gw)
            teams_averages_back_fill.run(season, gw)
            players_averages_back_fill.run(season, gw)
            season_averages_back_fill.run(season)
            fpl_points_predictor_back_fill.run(season, gw)
        if gw == 38:
            fpl_current_players_attributes_process_back_fill.run(season, gw)
            fpl_players_stats_process_back_fill.run(season, gw)
        if 1 <= gw <= 37:
            fpl_current_players_attributes_process_back_fill.run(season, gw)
            fpl_players_stats_process_back_fill.run(season, gw)
            teams_averages_back_fill.run(season, gw)
            players_averages_back_fill.run(season, gw)
            season_averages_back_fill.run(season)
            fpl_points_predictor_back_fill.run(season, gw)
