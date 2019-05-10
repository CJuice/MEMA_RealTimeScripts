"""

"""


def main():

    # IMPORTS
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    from dateutil import parser as date_parser
    import configparser
    import json
    import numpy as np
    import os
    import pyodbc
    import re
    import requests
    import xml.etree.ElementTree as ET

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_RITISBottlenecks.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    current_year = datetime.now().year
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    date_time_format = "%Y-%m-%d %H:%M:%S"
    mema_cfg_section_name = "MEMA_VALUES"
    realtime_webeocshelters_tbl = "[{database_name}].[dbo].[RealTime_RITISBottlenecks]"  # TODO: Check
    feature_objects_list = []
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'"""
    task_name = "RITISBottlenecks"  # TODO: Check

    TESTING = False  # OPTION

    print(f"Variables completed.")

    # ASSERTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

    # FUNCTIONS
    def clean_string_of_apostrophes_for_sql(value):
        """
        Replace an apostrophe in the string value with double apostrphes suitable for sql insertion

        old cgis process protected against apostrophes in description text to prevent issues with inserting into sql
        database. Saw no examples of apostrophes during development but chose to include protection as a precaution.
        :param value:
        :return:
        """
        return value.replace("'", "''")

    def create_date_time_value_for_db() -> str:
        """
        Create a formatted date and time value as string
        :return: string date & time
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def create_geometry_string_value(coordinate_pairs_list: list, geom_type: str) -> str:
        number_values = ", ".join([f"{lat} {lon}" for lat, lon in coordinate_pairs_list])
        return f"geometry::STGeomFromText('{geom_type.upper()}({number_values})', 4326)"

    def determine_database_config_value_based_on_script_name() -> str:
        """
        Inspect the python script file name to see if it includes _PROD and return appropriate value.
        During redesign there was a DEV and PROD version and each wrote to a different database. When manually
        deploying there was opportunity to error because the variable value had to be manually switched. Now all that
        has to happen is the file name has to be switched and the correct config file section is accessed.
        :return: string value for config file section to be accessed for database identity
        """

        file_name, extension = os.path.splitext(os.path.basename(__file__))
        if "_PROD" in file_name:
            return "DATABASE_PROD"
        else:
            return "DATABASE_DEV"

    def process_date_time_strings(value):
        return date_parser.parse(value).strftime(date_time_format)

    def setup_config(cfg_file: str) -> configparser.ConfigParser:
        """
        Instantiate the parser for accessing a config file.
        :param cfg_file: config file to access
        :return:
        """
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(filenames=cfg_file)
        return cfg_parser

    def time_elapsed(start=datetime.now()):
        """
        Calculate the difference between datetime.now() value and a start datetime value
        :param start: datetime value
        :return: datetime value
        """
        return datetime.now() - start

    # CLASSES
    @dataclass
    class Feature:
        """
        Data class for holding essential values about a RITIS Bottleneck Feature; most values inserted into SQL database
        """
        city: str
        closed_time: str
        county_id: str
        data_gen: str
        description: str
        geometry: str
        id: str
        length: str
        start_time: str
        zip_code: str
        state_id: str  # MD Fips always 24. This process filters for MD only; Is constant but didn't make default here.

    # FUNCTIONALITY
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # need mema specific values for post requests
    mema_request_header = json.loads(config_parser[mema_cfg_section_name]["HEADER"])
    mema_request_url = config_parser[mema_cfg_section_name]["URL"]
    mema_data = config_parser[mema_cfg_section_name]["DATA"]

    # need to make requests to mema url to get json for interrogation and data extraction
    try:
        if not TESTING:
            response = requests.post(url=mema_request_url, data=mema_data, headers=mema_request_header)
            print(response.status_code)
    except Exception as e:
        print(f"Exception during request for page {mema_request_url}. {e}")
        print(f"Response status code: {response.status_code}")
        print(f"Time elapsed {time_elapsed(start=start)}")
        exit()
    else:
        if TESTING:
            with open(r"Docs/ExampleJSONresponse.json", 'r') as handler:
                response_json = json.load(handler)
        else:
            response_json = response.json()
    try:
        header_dict = response_json.get("header", None)
        data_gen = header_dict.get("timestamp", None)
        data_gen_parsed = process_date_time_strings(value=data_gen)
    except AttributeError as ae:
        print(f"Error extracting data generated date and time value. \n{ae}\nMay be issue with response json: \n{response_json}")
        exit()

    features = response_json.get("features", None)
    for feature in features:
        try:
            id = feature.get("id", None)
        except AttributeError as ae:

            # If process can't get an id value then can't create unique feature object so continue on
            print(f"Error extracting feature id. Feature skipped {ae}\n\t{feature}")
            continue
        try:
            geometry = feature.get("geometry", None)
            coordinates = geometry.get("coordinates", None)
            geometry_type = geometry.get("type", None)
            geometry_string = create_geometry_string_value(coordinate_pairs_list=coordinates, geom_type=geometry_type)
            properties_dict = feature.get("properties", None)[0]  # List of length 1 at time of design
            length = properties_dict.get("length", None)
            start_time = properties_dict.get("startTimestamp", None)
            start_time_parsed = process_date_time_strings(value=start_time)
            closed_time = properties_dict.get("closedTimestamp", None)
            closed_time_parsed = process_date_time_strings(value=closed_time)
            location_dict = properties_dict.get("location", None)
            description = location_dict.get("description", None)
            description_cleaned = clean_string_of_apostrophes_for_sql(value=description)  # see function for note
            city = location_dict.get("city", None)
            zip_code = location_dict.get("zipcode", None)
            # state_id = location_dict.get("state", None)[0].get("fips", None)  # MD fips is always 24
            county_dict = location_dict.get("county", None)[0]  # List of length 1 at time of design
            county_id = county_dict.get("fips", None)
        except AttributeError as ae:

            """Protecting against an issue in all the extractison above. If can get an id, then proceed and try others.
                As long as have an id then I can make a unique object for database entry and also will help identify
                the feature(s) with issues in their json objects.
            """
            print(f"Error in attribute extraction from json. One of the expected values was not found. {ae}")
            str_None = str(None)
            feature_objects_list.append(Feature(city=str_None,
                                                closed_time=str_None,
                                                county_id=str_None,
                                                data_gen=str_None,
                                                description=str_None,
                                                geometry=str_None,
                                                id=id,
                                                length=str_None,
                                                start_time=str_None,
                                                state_id='24',  # Note, is a default value. MD fips is always 24.
                                                zip_code=str_None
                                                )
                                        )
        else:
            feature_objects_list.append(Feature(city=city,
                                                closed_time=closed_time_parsed,
                                                county_id=county_id,
                                                data_gen=data_gen_parsed,
                                                description=description_cleaned,
                                                geometry=geometry_string,
                                                id=id,
                                                length=length,
                                                start_time=start_time_parsed,
                                                state_id='24',  # Note, is a default value. MD fips is always 24.
                                                zip_code=zip_code
                                                )
                                        )
    # for obj in feature_objects_list:
    #     print(obj)

    return


if __name__ == "__main__":
    main()
