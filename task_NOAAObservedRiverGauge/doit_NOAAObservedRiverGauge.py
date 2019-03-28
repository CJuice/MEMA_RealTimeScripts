"""

"""


def main():

    # IMPORTS
    # from pprint import pprint
    # from Original_Scripts.cgis_Configs import getConfig
    # from Original_Scripts.cgis_Configs import getDBConnection
    # from Original_Scripts.cgis_Requests import simpleGetRequest
    # from Original_Scripts.cgis_Parsers import parseJsonResponse
    # from Original_Scripts.cgis_Transforms import transformNOAAObservedRiverGauges
    # from Original_Scripts.cgis_Updates import updateTaskTracking
    # from Original_Scripts.cgis_Configs import getMappingInfo
    # from Original_Scripts.cgis_MapToSQL import applyDataMap
    # from Original_Scripts.cgis_Updates import runSQL
    # from time import strftime
    # import sys
    # import traceback

    from datetime import datetime
    import configparser
    from dataclasses import dataclass
    import numpy as np
    import os
    import pandas as pd
    from pprint import pprint
    import pyodbc
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file_path = r"doit_config_NOAAObservedRiverGauge.cfg"
    # noaa_url = r"https://idpgis.ncep.noaa.gov/arcgis/rest/services/NWS_Observations/ahps_riv_gauges/MapServer/0/query?where=state%3D%27MD%27&outFields=state%2Clocation%2Cwfo%2CObserved%2Cstatus%2Cflood%2Cmoderate%2Cmajor&returnGeometry=true&f=pjson"
    noaa_url = r"https://idpgis.ncep.noaa.gov/arcgis/rest/services/NWS_Observations/ahps_riv_gauges/MapServer/0/query?"
    noaa_query_payload = {"where": "state = 'MD'",
                          "outFields": "gaugelid,state,location,observed,obstime,status,flood,moderate,major",
                          "returnGeometry": "true",
                          "f": "pjson"}
    NOAAObservedRiverGaugesInfo = {
        "details":
            {"tablename": "RealTime_NOAAObservedRiverGauges"},
        "mapping": [
            {"input": "location", "output": "Location", "type": "string"},
            {"input": "status", "output": "Status", "type": "string"},
            {"input": "x", "output": "X", "type": "float"},
            {"input": "y", "output": "Y", "type": "float"},
            {"input": "gaugelid", "output": "gaugeid", "type": "string"},
            {"input": "DataGenerated", "output": "DataGenerated", "type": "datetime %Y-%m-%d %H:%M:%S"}

        ]
    }
    realtime_noaaobservedrivergauge_headers = ("GaugeID", "Location", "Status", "X", "Y", "DataGenerated")
    realtime_noaaobservedrivergauge_tbl = "[{database_name}].[dbo].[RealTime_NOAAObservedRiverGauges]"
    sql_delete_insert_template = """DELETE FROM {realtime_noaaobservedrivergauge_tbl}; INSERT INTO {realtime_noaaobservedrivergauge_tbl} ({headers_joined}) VALUES """
    sql_statements_list = []
    sql_values_statement = """({values})"""
    sql_values_string_template = """'{location}', '{status}', '{gaugelid}','{longitude}', '{latitude}', '{data_gen}'"""
    database_cfg_section_name = "DATABASE_DEV"
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"


    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Guage:
        location: str
        status: str
        gaugelid: str
        latitude: str
        longitude: str
        data_gen: str

    # FUNCTIONS
    def create_database_connection_string(db_name: str, db_user: str, db_password: str) -> str:
        """
        Create the connection string for accessing database and return.
        :return: string, sql connection
        """
        return database_connection_string.format(database_name=db_name,
                                                 database_user=db_user,
                                                 database_password=db_password)

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
    # need a current datetime stamp for database entry
    current_date_time = str(create_date_time_value())
    print(f"Process Date & Time: {current_date_time}")

    # need parser to access credentials
    parser = setup_config(config_file_path)

    # Make request to url
    try:
        response = requests.get(url=noaa_url, params=noaa_query_payload)
    except Exception as e:
        print(f"Exception during request for html page {noaa_url}. {e}")
        exit()
    else:
        print(f"Response status code: {response.status_code}")
        response_json = response.json()
        # pprint(response_json)

    gauge_objects_list = []
    features = response_json["features"]
    for feature in features:
        attributes = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        gauge_objects_list.append(Guage(attributes.get("location", None),
                                        attributes.get("status", None),
                                        attributes.get("gaugelid", None),
                                        geometry.get("x", None),
                                        geometry.get("y", None),
                                        attributes.get("obstime", None)
                                        )
                                  )

    for gauge_obj in gauge_objects_list:
        values = sql_values_string_template.format(location=gauge_obj.location,
                                                   status=gauge_obj.status,
                                                   gaugelid=gauge_obj.gaugelid,
                                                   latitude=gauge_obj.latitude,
                                                   longitude=gauge_obj.longitude,
                                                   data_gen=gauge_obj.data_gen)
        values_string = sql_values_statement.format(values=values)
        sql_statements_list.append(values_string)

    # Database Transactions
    print("Database operations initiated...")
    database_name = parser[database_cfg_section_name]["NAME"]
    database_password = parser[database_cfg_section_name]["PASSWORD"]
    database_user = parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_noaaobservedrivergauge_headers])
    sql_delete_insert_string = sql_delete_insert_template.format(
        realtime_noaaobservedrivergauge_tbl=realtime_noaaobservedrivergauge_tbl.format(database_name=database_name),
        headers_joined=headers_joined)

    # Build the entire SQL statement to be executed
    full_sql_string = sql_delete_insert_string + ",".join(sql_statements_list)

    with pyodbc.connect(full_connection_string) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(full_sql_string)
        except pyodbc.DataError:
            print(f"A value in the sql exceeds the field length allowed in database table: {full_sql_string}")
        else:
            connection.commit()

    print("Process completed.")


if __name__ == "__main__":
    main()
