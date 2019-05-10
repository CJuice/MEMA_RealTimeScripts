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
    # current_year = datetime.now().year
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    date_time_format = "%Y-%m-%d %H:%M:%S"
    mema_cfg_section_name = "MEMA_VALUES"
    realtime_ritisbottlenecks_tbl = "[{database_name}].[dbo].[RealTime_RITISBottleNecks]"
    feature_objects_list = []
    ritis_bottlenecks_headers = ("ID", "starttime", "closedtime", "length", "description", "city", "zipcode",
                                 "stateID", "countyID", "geometry", "DataGenerated")
    # sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_delete_template = """DELETE FROM {table};"""
    sql_insert_template = """INSERT INTO {table} ({headers_joined}) VALUES """
    sql_insertion_step_increment = 1000
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{id}', '{start_time}', '{closed_time}', {length}, '{description}', '{city}', '{zip_code}', {state_id}, '{county_id}', {geometry}, '{data_gen}'"""
    task_name = "RITISBottleNecks"

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

    def sql_insert_generator(sql_values_list, step_increment, sql_insert_string):
        """
        Generator for yielding batches of sql values for insertion
        Purpose is to work with the 1000 record limit of SQL insertion.
        :param sql_values_list: list of prebuilt record values ready for sql insertion
        :param step_increment: the record count increment for insertion batches
        :param sql_insert_string: sql statement string for use with values
        :return: yield a string for use in insertion
        """
        for i in range(0, len(sql_values_list), step_increment):
            values_in_range = sql_values_list[i: i + step_increment]

            # Build the entire SQL statement to be executed
            output = sql_insert_string + ",".join(values_in_range)
            yield output

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
        city: str = np.NaN
        closed_time: str = np.NaN
        county_id: str = np.NaN
        data_gen: str = np.NaN
        description: str = np.NaN
        geometry: str = np.NaN
        id: str = np.NaN
        length: float = np.NaN
        start_time: str = np.NaN
        state_id: int = np.NaN  # MD Fips always 24. This process filters for MD only; Is constant but didn't make default here.
        zip_code: str = np.NaN

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
            # print(response.status_code)
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
            length = float(properties_dict.get("length", None))
            start_time = properties_dict.get("startTimestamp", None)
            start_time_parsed = process_date_time_strings(value=start_time)
            closed_time = properties_dict.get("closedTimestamp", None)
            closed_time_parsed = process_date_time_strings(value=closed_time)
            location_dict = properties_dict.get("location", None)
            description = location_dict.get("description", None)
            description_cleaned = clean_string_of_apostrophes_for_sql(value=description)  # see function for note
            city = location_dict.get("city", None)
            zip_code = location_dict.get("zipcode", None)
            # state_id = int(location_dict.get("state", None)[0].get("fips", None))  # MD fips is always 24
            county_dict = location_dict.get("county", None)[0]  # List of length 1 at time of design
            county_id = county_dict.get("fips", None)
        except AttributeError as ae:

            """Protecting against an issue in all the extractison above. If can get an id, then proceed and try others.
                As long as have an id then I can make a unique object for database entry and also will help identify
                the feature(s) with issues in their json objects.
            """
            print(f"Error in attribute extraction from json. One of the expected values was not found. {ae}")
            str_None = str(None)
            feature_objects_list.append(Feature(data_gen=str_None,
                                                id=id,
                                                state_id=24,  # Note, is a default value. MD fips is always 24.
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
                                                state_id=24,  # Note, is a default value. MD fips is always 24.
                                                zip_code=zip_code
                                                )
                                        )

    # Need to build the values string statements for use later on with sql insert statement.
    for feature_obj in feature_objects_list:
        values = sql_values_string_template.format(id=feature_obj.id,
                                                   start_time=feature_obj.start_time,
                                                   closed_time=feature_obj.closed_time,
                                                   length=feature_obj.length,
                                                   description=feature_obj.description,
                                                   city=feature_obj.city,
                                                   zip_code=feature_obj.zip_code,
                                                   state_id=feature_obj.state_id,
                                                   county_id=feature_obj.county_id,
                                                   geometry=feature_obj.geometry,
                                                   data_gen=feature_obj.data_gen)
        values_string = sql_values_statement.format(values=values)
        sql_values_statements_list.append(values_string)

    # Database Transactions
    print(f"\nDatabase operations initiated. Time elapsed {time_elapsed(start=start)}")
    database_name = config_parser[database_cfg_section_name]["NAME"]
    database_password = config_parser[database_cfg_section_name]["PASSWORD"]
    database_user = config_parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)
    database_table_name = realtime_ritisbottlenecks_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in ritis_bottlenecks_headers])
    sql_delete_string = sql_delete_template.format(table=database_table_name)
    sql_insert_string = sql_insert_template.format(
        table=database_table_name,
        headers_joined=headers_joined)

    # Need the insert statement generator to be ready for database insertion rounds
    sql_insert_gen = sql_insert_generator(sql_values_list=sql_values_statements_list,
                                          step_increment=sql_insertion_step_increment,
                                          sql_insert_string=sql_insert_string)

    # Build the sql for updating the task tracker table for this process.
    sql_task_tracker_update = f"UPDATE RealTime_TaskTracking SET lastRun = '{start_date_time}', DataGenerated = (SELECT max(DataGenerated) from {database_table_name}) WHERE taskName = '{task_name}'"

    with pyodbc.connect(full_connection_string) as connection:
        cursor = connection.cursor()

        # Due to 1000 record insert limit, delete records first and then do insertion rounds for 2400+ gauges
        try:
            cursor.execute(sql_delete_string)
        except Exception as e:
            print(f"Error deleting records from {database_table_name}. {e}")
            exit()
        else:
            print(f"Delete statement executed. Time elapsed {time_elapsed(start=start)}")

        # Need insert statement in rounds of 1000 records or less to avoid sql limit
        insert_round_count = 1
        for batch in sql_insert_gen:
            try:
                cursor.execute(batch)
            except pyodbc.DataError:
                print(f"A value in the sql exceeds the field length allowed in database table: {batch}")
            else:
                print(f"Executing insert batch {insert_round_count}. Time elapsed {time_elapsed(start=start)}")
                insert_round_count += 1

        # Need to update the task tracker table to record last run time
        try:
            cursor.execute(sql_task_tracker_update)
        except pyodbc.DataError:
            print(
                f"A value in the sql exceeds the field length allowed in database table: {sql_task_tracker_update}")

        connection.commit()
        print(f"Commit successful. Time elapsed {time_elapsed(start=start)}")

    print("\nProcess completed.")
    print(f"Time elapsed {time_elapsed(start=start)}")


if __name__ == "__main__":
    main()
