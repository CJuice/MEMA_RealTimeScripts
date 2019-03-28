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
    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Guage:
        location: str
        status: str
        gaugelid: str
        x: str
        y: str
        data_gen: str

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
    for obj in gauge_objects_list:
        print(obj)

    return


if __name__ == "__main__":
    main()
