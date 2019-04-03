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
    import pandas as pd
    import requests
    import os
    from dateutil import parser as date_parser

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
        state_abbrev: str
        site_name: str
        site_code: str
        discharge: str
        gauge_height: str
        data_gen: str
        collect_date: str

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
            print(f"extract_variable_value(): {e}")
            return np.NaN

    def extract_collected_date(second_level_json):
        try:
            return second_level_json.get("dateTime", np.NaN)
        except Exception as e:
            print(f"extract_collected_date(): {e}")
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
            return np.NaN

    def extract_data_generated_value(value_json):
        try:
            result1 = value_json.get("queryInfo",{})
            result2 = result1.get("note", [])
            result3 = result2[3]
            return result3.get("value", np.NaN)
        except Exception as e:
            print(f"extract_data_generated_value(): {e}")
            return np.NaN

    def determine_gauge_height_value(variable_code, variable_value):
        if variable_code == "00060":
            return np.NaN
        if pd.isnull(variable_value):
            return np.NaN
        return variable_value

    def determine_discharge_value(variable_code, variable_value):
        if variable_code == "00065":
            return np.NaN
        if pd.isnull(variable_value):
            return np.NaN
        return variable_value

    def process_site_code(site_code):
        if pd.notnull(site_code):
            return site_code

    def process_date_string(date_string):
        return date_parser.parse(date_string).strftime('%Y-%m-%d %H:%M:%S')

    def time_elapsed(start=datetime.now()):
        """
        Calculate the difference between datetime.now() value and a start datetime value
        :param start: datetime value
        :return: datetime value
        """
        return datetime.now() - start

    # FUNCTIONALITY
    start = datetime.now()
    print(f"Process started: {start}")

    # need a current datetime stamp for process printout
    # current_date_time = str(create_date_time_value())
    # print(f"Process Date & Time: {current_date_time}")

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url and alter the state being requested
    for state_abbrev in state_abbreviations_list:
        usgs_query_payload["stateCd"] = state_abbrev
        print(f"\nProcessing {state_abbrev.upper()}; Time elapsed {time_elapsed(start=start)}")

        try:
            response = requests.get(url=usgs_url, params=usgs_query_payload)
        except Exception as e:
            print(f"Exception during request for html page {usgs_url}. {e}")
            print(f"Response status code: {response.status_code}")
            print(response.url)
            print(f"Time elapsed {time_elapsed(start=start)}")
            exit()
        else:
            response_json = response.json()
            value_json = response_json.get("value", {})
            time_series_json = value_json.get("timeSeries", {})
            data_gen_value = extract_data_generated_value(value_json=value_json)
            data_gen_processed = process_date_string(date_string=data_gen_value)
            for gauge_json in time_series_json:
                source_info_json = extract_source_info(gauge_json=gauge_json)
                second_level_values_json = extract_second_level_values(gauge_json=gauge_json)
                site_name = extract_site_name(source_info_json=source_info_json)
                site_code = extract_site_code(source_info_json=source_info_json)
                site_code_processed = process_site_code(site_code=site_code)
                variable_code = extract_variable_code(gauge_json=gauge_json)
                variable_value = extract_variable_value(second_level_json=second_level_values_json)
                collected_date = extract_collected_date(second_level_json=second_level_values_json)
                collected_date_processed = process_date_string(date_string=collected_date)
                gauge_height = determine_gauge_height_value(variable_code=variable_code, variable_value=variable_value)
                discharge = determine_discharge_value(variable_code=variable_code, variable_value=variable_value)
                gauge_objects_list.append(Gauge(state_abbrev=state_abbrev,
                                                site_name=site_name,
                                                site_code=site_code_processed,
                                                discharge=discharge,
                                                gauge_height=gauge_height,
                                                data_gen=data_gen_processed,
                                                collect_date=collected_date_processed))
            # for obj in gauge_objects_list:
            #     print(obj)

    print("\nProcess completed.")
    print(f"Time elapsed {time_elapsed(start=start)}")


if __name__ == "__main__":
    main()