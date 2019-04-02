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
    from dataclasses import dataclass
    from datetime import datetime
    import configparser
    import numpy as np
    import requests
    import os

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_USGSStreamGauge.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_cfg_section_name = "DATABASE_DEV"
    gauge_objects_list = []
    usgs_url = r"http://waterservices.usgs.gov/nwis/iv/"
    usgs_query_payload = {"format": "json",
                          "stateCd": None,
                          "parameterCd": "00060,00065",
                          "siteStatus": "active"}
    state_abbreviations_list = ["md", "dc", "de", "pa", "wv", "va", "nc", "sc"]

    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Gauge:
        site_name: str
        site_code: str
        variable_code: str
        variable_value: str
        created_date: str

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

    def extract_site_name(source_info_json):
        try:
            return source_info_json.get("siteName", np.NaN)
        except Exception as e:
            print(f"extract_site_name(): {e}")
            return np.NaN

    def extract_site_code(source_info_json):
        try:
            result1 = source_info_json.get("siteCode", [])
            result2 = result1[0]
            return result2.get("value", np.NaN)
        except Exception as e:
            print(f"extract_site_code(): {e}")
            return np.NaN

    def extract_variable_code(gauge_json):
        try:
            result1 = gauge_json.get("variable", {})
            result2 = result1.get("variableCode", {})
            result3 = result2[0]
            return result3.get("value", np.NaN)
        except Exception as e:
            print(f"extract_variable_code(): {e}")
            return np.NaN

    def extract_variable_value(second_level_json):
        try:
            return second_level_json.get("value", np.NaN)
        except Exception as e:
            print(f"extract_variable_code(): {e}")
            return np.NaN

    def extract_created_date(second_level_json):
        try:
            return second_level_json.get("dateTime", np.NaN)
        except Exception as e:
            print(f"extract_variable_code(): {e}")
            return np.NaN

    def extract_source_info(gauge_json):
        try:
            return gauge_json.get("sourceInfo", {})
        except Exception as e:
            print(f"extract_source_info(): {e}")
            return {}

    def extract_second_level_values(gauge_json):
        try:
            result1 = gauge_json.get("values", [])
            result2 = result1[0]
            result3 = result2.get("value", [])
            return result3[0]
        except Exception as e:
            print(f"extract_second_level_values(): {e}")
            return {}

    # FUNCTIONALITY
    # need a current datetime stamp for process printout
    current_date_time = str(create_date_time_value())
    print(f"Process Date & Time: {current_date_time}")

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url and alter the state being requested
    for state_abbrev in state_abbreviations_list:
        usgs_query_payload["stateCd"] = state_abbrev
        try:
            response = requests.get(url=usgs_url, params=usgs_query_payload)
        except Exception as e:
            print(f"Exception during request for html page {usgs_url}. {e}")
            print(response.url)
            exit()
        else:
            print(f"Response status code: {response.status_code}")
            response_json = response.json()
            value_json = response_json.get("value", {})
            time_series_json = value_json.get("timeSeries", {})
            for gauge_json in time_series_json:
                source_info_json = extract_source_info(gauge_json=gauge_json)
                second_level_values_json = extract_second_level_values(gauge_json=gauge_json)
                site_name = extract_site_name(source_info_json=source_info_json)
                site_code = extract_site_code(source_info_json=source_info_json)
                variable_code = extract_variable_code(gauge_json=gauge_json)
                variable_value = extract_variable_value(second_level_json=second_level_values_json)
                created_date = extract_created_date(second_level_json=second_level_values_json)

            exit()
            # source_info_vals = response_json.get("sourceInfo", {})
            # variable_vals = response_json.get("variable", {})
            # values_vals = response_json.get("values", {})
            # print(time_series_json)

    return


if __name__ == "__main__":
    main()