"""
TODO: Documentation
"""


def main():
    # IMPORTS
    # from Original_Scripts.cgis_Configs import getConfig
    # from Original_Scripts.cgis_Configs import getDBConnection
    # from Original_Scripts.cgis_Transforms import transformStreamGages
    # from Original_Scripts.cgis_Requests import simpleGetRequest
    # from Original_Scripts.cgis_Parsers import parseJsonResponse
    # from Original_Scripts.cgis_MapToSQL import applyDataMap
    # from Original_Scripts.cgis_Configs import getMappingInfo
    # import Original_Scripts.cgis_Updates as cgis_Updates
    # from time import strftime
    # import sys
    # import traceback
    from datetime import datetime
    import configparser
    import requests
    import os

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_USGSStreamGauge.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_cfg_section_name = "DATABASE_DEV"
    usgs_url = r"http://waterservices.usgs.gov/nwis/iv/"
    usgs_query_payload = {"format": "json",
                          "stateCd": None,
                          "parameterCd": "00060,00065",
                          "siteStatus": "active"}
    state_abbreviations_list = ["md", "dc", "de", "pa", "wv", "va", "nc", "sc"]

    # ASSERTS
    assert os.path.exists(config_file_path)

    # FUNCTIONS
    def create_date_time_value() -> str:
        """
        Create a formatted date and time value as string
        :return: string date & time
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def setup_config(cfg_file: str) -> configparser.ConfigParser:
        """
        Instantiate the parser for accessing a config file.
        :param cfg_file: config file to access
        :return:
        """
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(filenames=cfg_file)
        return cfg_parser

    # FUNCTIONALITY
    # need a current datetime stamp for process printout
    current_date_time = str(create_date_time_value())
    print(f"Process Date & Time: {current_date_time}")

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url and alter the state being requested
    for state_abbrev in state_abbreviations_list:
        usgs_query_payload["stateCd"] = state_abbrev
        print(state_abbrev)
        try:
            response = requests.get(url=usgs_url, params=usgs_query_payload)
        except Exception as e:
            print(f"Exception during request for html page {usgs_url}. {e}")
            print(response.url)
            exit()
        else:
            print(f"Response status code: {response.status_code}")
            response_json = response.json()
        # print(response_json)
    return


if __name__ == "__main__":
    main()