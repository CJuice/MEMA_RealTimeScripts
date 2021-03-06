"""
This is a procedural script for populating MEMA database with USGS stream gauge data.

This process makes request to USGS web services. It captures the site number, collected date, data generated date,
and determines the discharge, gauge height, and status from response JSON.
Gauge Dataclass objects are created with these values and stored in a list. The list of objects is accessed and used to
generate the values in the insert sql statement. Once the insert statement is completed a database connection
is established, all existing records are deleted, and the new records are inserted. The number of gauge records
to be inserted exceeds the 1000 record sql limit so insert statements happen in rounds of 1000 records. At time
of design there were over 2400 gauges.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190404
Revisions:

"""


def main():

    # IMPORTS
    from dataclasses import dataclass
    from datetime import datetime
    from dateutil import parser as date_parser
    import configparser
    import numpy as np
    import os
    import pandas as pd
    import pyodbc
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_USGSStreamGauge.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    gauge_objects_list = []
    realtime_usgsstreamgauge_tbl = "[{database_name}].[dbo].[RealTime_USGSStreamGages]"
    sql_delete_template = """DELETE FROM {table};"""
    sql_insert_template = """INSERT INTO {table} ({headers_joined}) VALUES """
    sql_insertion_step_increment = 1000
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{site_number}', '{discharge}', '{gauge_height}','{status}', '{collected_date}', '{data_gen}'"""
    state_abbreviations_list = ["md", "dc", "de", "pa", "wv", "va", "nc", "sc"]
    task_name = "USGSStreamGages"
    usgs_query_payload = {"format": "json",
                          "stateCd": None,
                          "parameterCd": "00060,00065",
                          "siteStatus": "active"}
    usgs_streamgauge_headers = ("SiteNumber", "Discharge", "GageHeight", "Status", "collectedDate", "DataGenerated")
    usgs_url = r"http://waterservices.usgs.gov/nwis/iv/"

    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Gauge:
        """
        Data class for holding essential values about a Gauge; most values inserted into SQL database
        """
        state_abbrev: str
        site_name: str
        site_code: str
        discharge: float
        gauge_height: float
        data_gen: str
        collect_date: str

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

    def determine_discharge_value(variable_code, variable_value) -> float:
        """
        Determine the discharge value based on the variable code and value values.
        This was inherited logic from the old CGIS code. There were no notes on the basis of this design so the
        design was brought into the new flow for consistency.
        :param variable_code: value from response JSON
        :param variable_value: value from response JSON
        :return: float
        """
        if variable_code == "00065":
            return -9999
        if pd.isnull(variable_value):
            return -9999
        return float(variable_value)

    def determine_gauge_height_value(variable_code, variable_value) -> float:
        """
        Determine the gauge height value based on the variable code and value values.
        This was inherited logic from the old CGIS code. There were no notes on the basis of this design so the
        design was brought into the new flow for consistency.
        :param variable_code: value from response JSON
        :param variable_value: value from response JSON
        :return: float
        """
        if variable_code == "00060":
            return -9999
        if pd.isnull(variable_value):
            return -9999
        return float(variable_value)

    def extract_collected_date(second_level_json):
        """
        Extract the value associated with the 'dateTime' key in the json
        :param second_level_json: json resulting from two levels of value(s) key extraction
        :return: value or numpy nan
        """
        try:
            return second_level_json.get("dateTime", np.NaN)
        except Exception as e:
            print(f"extract_collected_date(): {e}")
            return np.NaN

    def extract_data_generated_value(value_json):
        """
        Extract the data generated value from the response json
        :param value_json: json from 'value' key in response json
        :return: value or numpy nan
        """
        try:
            result1 = value_json.get("queryInfo", {})
            result2 = result1.get("note", [])
            result3 = result2[3]
            return result3.get("value", np.NaN)
        except Exception as e:
            print(f"extract_data_generated_value(): {e}")
            return np.NaN

    def extract_second_level_values(gauge_json):
        """
        Multiple extractions of keys and values from gauge json object to get second level values for further use
        :param gauge_json: gauge json object from the time series json
        :return: value or numpy nan
        """
        try:
            result1 = gauge_json.get("values", [])
            result2 = result1[0]
            result3 = result2.get("value", [])
            return result3[0]
        except Exception as e:
            print(f"extract_second_level_values(): {e}")
            return np.NaN

    def extract_site_code(source_info_json):
        """
        Extract the value associated with the 'siteCode' key in the json
        :param source_info_json: json resulting from 'sourceInfo' key extraction
        :return: value or numpy nan
        """
        try:
            result1 = source_info_json.get("siteCode", [])
            result2 = result1[0]
            return result2.get("value", np.NaN)
        except Exception as e:
            print(f"extract_site_code(): {e}")
            return np.NaN

    def extract_site_name(source_info_json):
        """
        Extract the value associated with the 'siteName' key in the json
        :param source_info_json: json resulting from 'sourceInfo' key extraction
        :return: value or numpy nan
        """
        try:
            return source_info_json.get("siteName", np.NaN)
        except Exception as e:
            print(f"extract_site_name(): {e}")
            return np.NaN

    def extract_source_info(gauge_json):
        """
        Extract the value associate with the 'sourceInfo' key in the json
        :param gauge_json: gauge json object from the time series json
        :return: value or empty dict
        """
        try:
            return gauge_json.get("sourceInfo", {})
        except Exception as e:
            print(f"extract_source_info(): {e}")
            return {}

    def extract_variable_code(gauge_json):
        """
        Multiple extractions of keys and values from gauge json object to get the variable code value
        :param gauge_json: gauge json object from the time series json
        :return: value or numpy nan
        """
        try:
            result1 = gauge_json.get("variable", {})
            result2 = result1.get("variableCode", {})
            result3 = result2[0]
            return result3.get("value", np.NaN)
        except Exception as e:
            print(f"extract_variable_code(): {e}")
            return np.NaN

    def extract_variable_value(second_level_json):
        """
        Extract the variable value from the second level json
        :param second_level_json: json resulting from two levels of value(s) key extraction
        :return:
        """
        try:
            return second_level_json.get("value", np.NaN)
        except Exception as e:
            print(f"extract_variable_value(): {e}")
            return np.NaN

    def process_date_string(date_string):
        """
        Parse the date string to datetime format using the dateutil parser and return string formatted
        Old CGIS way was to manipulate string by removing a 'T' and doing other actions instead of using module
        :param date_string: string extracted from response json
        :return: date/time string formatted as indicated
        """
        return date_parser.parse(date_string).strftime('%Y-%m-%d %H:%M:%S')

    def process_site_code(site_code):
        """
        Determine if the site code value is null or not null
        :param site_code: value from response json extraction
        :return: value or numpy nan
        """
        if pd.notnull(site_code):
            return site_code
        else:
            return np.NaN

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

    # FUNCTIONALITY
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url and alter the US state being requested
    for state_abbrev in state_abbreviations_list:
        usgs_query_payload["stateCd"] = state_abbrev
        print(f"\nProcessing {state_abbrev.upper()}. Time elapsed {time_elapsed(start=start)}")

        try:
            response = requests.get(url=usgs_url, params=usgs_query_payload)
        except Exception as e:
            print(f"Exception during request for html page {usgs_url}. {e}")
            print(f"Response status code: {response.status_code}")
            print(response.url)
            print(f"Time elapsed {time_elapsed(start=start)}")
            exit()
        else:
            # extract values from response json for gauge json object interrogation and Gauge object creation
            response_json = response.json()
            value_json = response_json.get("value", {})
            time_series_json = value_json.get("timeSeries", {})

            data_gen = extract_data_generated_value(value_json=value_json)
            data_gen_processed = process_date_string(date_string=data_gen)

            # Need to iterate over gauge objects in time series json and extract/process data for values of interest
            for gauge_json in time_series_json:
                source_info_json = extract_source_info(gauge_json=gauge_json)
                second_level_values_json = extract_second_level_values(gauge_json=gauge_json)
                site_name = extract_site_name(source_info_json=source_info_json)
                site_code = extract_site_code(source_info_json=source_info_json)
                variable_code = extract_variable_code(gauge_json=gauge_json)
                variable_value = extract_variable_value(second_level_json=second_level_values_json)
                collected_date = extract_collected_date(second_level_json=second_level_values_json)
                collected_date_processed = process_date_string(date_string=collected_date)
                discharge = determine_discharge_value(variable_code=variable_code, variable_value=variable_value)
                gauge_height = determine_gauge_height_value(variable_code=variable_code, variable_value=variable_value)
                site_code_processed = process_site_code(site_code=site_code)

                # Need to build the Gauge objects and store for use in sql inseration
                gauge_objects_list.append(Gauge(state_abbrev=state_abbrev,
                                                site_name=site_name,
                                                site_code=site_code_processed,
                                                discharge=discharge,
                                                gauge_height=gauge_height,
                                                data_gen=data_gen_processed,
                                                collect_date=collected_date_processed))

    # Need to build the values string statements for use later on with sql insert statement.
    for gauge_obj in gauge_objects_list:
        values = sql_values_string_template.format(site_number=gauge_obj.site_code,
                                                   discharge=gauge_obj.discharge,
                                                   gauge_height=gauge_obj.gauge_height,
                                                   status=np.NaN,
                                                   collected_date=gauge_obj.collect_date,
                                                   data_gen=gauge_obj.data_gen)
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
    database_table_name = realtime_usgsstreamgauge_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in usgs_streamgauge_headers])
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
            print(f"A value in the sql exceeds the field length allowed in database table: {sql_task_tracker_update}")

        connection.commit()
        print(f"Commit successful. Time elapsed {time_elapsed(start=start)}")

    # Old process executed a stored procedure for updating the Gauge locations table with status information based
    #   on business logic in the stored procedure.
    with pyodbc.connect(full_connection_string) as connection:
        cursor = connection.cursor()

        try:
            cursor.execute("exec RealTime_UpdateUSGSStreamGagesStatus")
        except Exception as e:
            print(f"Error executing stored procedure RealTime_UpdateUSGSStreamGagesStatus. {e}")
            exit()
        else:
            connection.commit()
            print(f"Stored procedure executed. Time elapsed {time_elapsed(start=start)}")

    print("\nProcess completed.")
    print(f"Time elapsed {time_elapsed(start=start)}")


if __name__ == "__main__":
    main()