import json
import requests
from config import ConfigurationParser

_fpl_events_endpoint = ConfigurationParser.get_config("external", "fpl_main_uri")


def get_current_gw():
    events_response = requests.get(_fpl_events_endpoint)
    events_response.raise_for_status()
    events_data = json.loads(events_response.text)["events"]
    gw_num = 0
    for event in events_data:
        if event["is_current"]:
            gw_num = event["id"]

    return gw_num
