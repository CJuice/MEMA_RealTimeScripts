"""

"""


def main():

    # IMPORTS
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
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
    mema_cfg_section_name = "MEMA_VALUES"
    realtime_webeocshelters_tbl = "[{database_name}].[dbo].[RealTime_RITISBottlenecks]"  # TODO: Check
    feature_objects_list = []
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'"""
    task_name = "RITISBottlenecks"  # TODO: Check

    TESTING = True  # OPTION

    print(f"Variables completed.")

    # ASSERTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

    # FUNCTIONS
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
        state_id: str = "24"  # MD Fips always 24. This process filters for MD only; So constant unless redesigned

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

        features = response_json.get("features")
        for feature in features:
            id = feature.get("id", None)
            geometry = feature.get("geometry", None)
            coordinates = geometry.get("coordinates", None)
            geometry_type = geometry.get("type", None)
            geometry_string = create_geometry_string_value(coordinate_pairs_list=coordinates, geom_type=geometry_type)
            properties_dict = feature.get("properties", None)[0]  # List of length 1 at time of design
            length = properties_dict.get("length", None)
            location_dict = properties_dict.get("location", None)
            city = location_dict.get("city", None)
            zip_code = location_dict.get("zipcode", None)
            # state_id = location_dict.get("state", None)[0].get("fips", None)  # MD fips is always 24
            county_dict = location_dict.get("county", None)[0]  # List of length 1 at time of design
            county_id = county_dict.get("fips", None)

            continue
            # feature_objects_list.append(Feature(city=,
            #                                     closed_time=,
            #                                     county_id=,
            #                                     data_gen=,
            #                                     description=,
            #                                     geometry=,
            #                                     id=,
            #                                     length=,
            #                                     start_time=,
            #                                     state_id=,
            #                                     zip_code=
            #                                     )
            #                             )
    # coordinates = features.get()



    

    return


if __name__ == "__main__":
    main()
