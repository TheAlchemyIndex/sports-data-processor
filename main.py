from jobs.averages import teams_averages, players_averages
from jobs.processed_data.players import fpl_current_players_attributes_process, fpl_players_stats_process


def main():
    # Raw data ingest
    # fpl_elements_ingest.run()
    # fpl_fixtures_ingest.run()
    # fpl_teams_ingest.run()
    # fpl_gw_ingest.run()
    #
    # # Processing data
    # fpl_fixtures_process.run()
    # fpl_team_stats_process.run()
    # fpl_current_players_attributes_process.run()
    # fpl_players_stats_process.run()

    # Averages
    # teams_averages.run()
    players_averages.run()


if __name__ == '__main__':
    main()
