"""
This is a procedural script for populating MEMA database with NOAA river gauge data.

This process queries an ArcGIS server REST endpoint. It queries for the gaugelid, state, location,
observed, obstime, status, flood, moderate, major, and geometry fields and gets the results as JSON.
The JSON is interrogated for the gaugelid, location, status, x, y, and obstime values. Gauge Dataclass
objects are created with these values and stored in a list. The list of objects is accessed and used to
generate the values in the insert sql statement. Once the insert statement is completed a database connection
is established, all existing records are deleted, and the new records are inserted.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190327
Revisions:

"""


def main():

    # IMPORTS
    from dataclasses import dataclass
    from datetime import datetime
    import configparser
    import os
    import pyodbc
    import requests
    from dateutil import parser as date_parser

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_NOAAObservedRiverGauge.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    gauge_objects_list = []
    noaa_query_payload = {"where": "state = 'MD'",
                          "outFields": "gaugelid,state,location,observed,obstime,status,flood,moderate,major",
                          "returnGeometry": "true",
                          "f": "pjson"}
    noaa_url = r"https://idpgis.ncep.noaa.gov/arcgis/rest/services/NWS_Observations/ahps_riv_gauges/MapServer/0/query?"
    realtime_noaaobservedrivergauge_headers = ("GaugeID", "Location", "Status", "X", "Y", "DataGenerated")
    realtime_noaaobservedrivergauge_tbl = "[{database_name}].[dbo].[RealTime_NOAAObservedRiverGauges]"
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{gaugelid}', '{location}', '{status}', {longitude}, {latitude}, '{data_gen}'"""
    task_name = "NOAAStreamGauges"

    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Gauge:
        location: str
        status: str
        gaugelid: str
        latitude: float
        longitude: float
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

    def create_date_time_value_for_db() -> str:
        """
        Create a formatted date and time value as string
        :return: string date & time
        """
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def datetime_quality_control(gauge_obj: Gauge) -> str:
        """
        Check the data generated value for the occasional N/A or any other unparsable value and set to new value
        :param gauge_obj: value from NOAA query results
        :return: string
        """
        try:
            converted = date_parser.parse(gauge_obj.data_gen)
        except ValueError as ve:
            converted = "1970-01-01 00:00:00"
            print(f"Gauge {gauge_obj.gaugelid} {gauge_obj.location} date value was invalid {gauge_obj.data_gen} -> {converted}")
        return str(converted)

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

    # FUNCTIONALITY
    # need a current datetime stamp for process printout
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url
    try:
        response = requests.get(url=noaa_url, params=noaa_query_payload)
    except Exception as e:
        print(f"Exception during request for html page {noaa_url}. {e}")
        exit()
    else:
        response_json = response.json()
        print(f"Response status code: {response.status_code}")
        print(f"Time elapsed {time_elapsed(start=start)}")

    features = response_json["features"]
    for feature in features:
        attributes = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        gauge_objects_list.append(Gauge(location=attributes.get("location", None),
                                        status=attributes.get("status", None),
                                        gaugelid=attributes.get("gaugelid", None),
                                        latitude=float(geometry.get("y", None)),
                                        longitude=float(geometry.get("x", None)),
                                        data_gen=attributes.get("obstime", None)
                                        )
                                  )

    for gauge_obj in gauge_objects_list:
        gauge_obj.data_gen = datetime_quality_control(gauge_obj)
        values = sql_values_string_template.format(location=gauge_obj.location,
                                                   status=gauge_obj.status,
                                                   gaugelid=gauge_obj.gaugelid,
                                                   latitude=gauge_obj.latitude,
                                                   longitude=gauge_obj.longitude,
                                                   data_gen=gauge_obj.data_gen)
        values_string = sql_values_statement.format(values=values)
        sql_values_statements_list.append(values_string)

    # Database Transactions
    print(f"Database operations initiated. Time elapsed {time_elapsed(start=start)}")
    database_name = config_parser[database_cfg_section_name]["NAME"]
    database_password = config_parser[database_cfg_section_name]["PASSWORD"]
    database_user = config_parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)
    realtime_noaaobservedrivergauge_tbl_string = realtime_noaaobservedrivergauge_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_noaaobservedrivergauge_headers])
    sql_delete_insert_string = sql_delete_insert_template.format(
        table=realtime_noaaobservedrivergauge_tbl_string,
        headers_joined=headers_joined)

    # Build the entire SQL statement to be executed
    full_sql_string = sql_delete_insert_string + ",".join(sql_values_statements_list)

    # Build the sql for updating the task tracker table for this process.
    sql_task_tracker_update = f"UPDATE RealTime_TaskTracking SET lastRun = '{start_date_time}', DataGenerated = (SELECT max(DataGenerated) from {realtime_noaaobservedrivergauge_tbl_string}) WHERE taskName = '{task_name}'"

    with pyodbc.connect(full_connection_string) as connection:
        cursor = connection.cursor()
        try:
            cursor.execute(full_sql_string)
            cursor.execute(sql_task_tracker_update)
        except pyodbc.DataError:
            print(f"A value in the sql exceeds the field length allowed in database table: {full_sql_string}")
        else:
            connection.commit()
            print(f"Commit successful. Time elapsed {time_elapsed(start=start)}")

    print("\nProcess completed.")
    print(f"Time elapsed {time_elapsed(start=start)}")


if __name__ == "__main__":
    main()
