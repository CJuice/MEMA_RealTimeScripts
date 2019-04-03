"""
TODO: Documentation
"""
# TODO: Add task tracker writing

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
    database_cfg_section_name = "DATABASE_DEV"
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    gauge_objects_list = []
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
    realtime_usgsstreamgauge_tbl = "[{database_name}].[dbo].[RealTime_USGSStreamGages]"
    usgs_url = r"http://waterservices.usgs.gov/nwis/iv/"

    streamGagesInfo = {
        "details":
            {"tablename": "RealTime_USGSStreamGages"},
        "mapping": [
            {"input": "collectedDate", "output": "collectedDate", "type": "datetime %Y-%m-%d %H:%M:%S"},
            {"input": "DataGenerated", "output": "DataGenerated", "type": "datetime %Y-%m-%d %H:%M:%S"},
            {"input": "siteCode", "output": "SiteNumber", "type": "string"},
            {"input": "gage_height", "output": "GageHeight", "type": "float"},
            {"input": "discharge", "output": "Discharge", "type": "float"},
        ]
    }

    # ASSERTS
    assert os.path.exists(config_file_path)

    # CLASSES
    @dataclass
    class Gauge:
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

    def determine_gauge_height_value(variable_code, variable_value):
        if variable_code == "00060":
            return -9999
        if pd.isnull(variable_value):
            return -9999
        return float(variable_value)

    def determine_discharge_value(variable_code, variable_value):
        if variable_code == "00065":
            return -9999
        if pd.isnull(variable_value):
            return -9999
        return float(variable_value)

    def extract_collected_date(second_level_json):
        try:
            return second_level_json.get("dateTime", np.NaN)
        except Exception as e:
            print(f"extract_collected_date(): {e}")
            return np.NaN

    def extract_site_code(source_info_json):
        try:
            result1 = source_info_json.get("siteCode", [])
            result2 = result1[0]
            return result2.get("value", np.NaN)
        except Exception as e:
            print(f"extract_site_code(): {e}")
            return np.NaN

    def extract_site_name(source_info_json):
        try:
            return source_info_json.get("siteName", np.NaN)
        except Exception as e:
            print(f"extract_site_name(): {e}")
            return np.NaN

    def extract_source_info(gauge_json):
        try:
            return gauge_json.get("sourceInfo", {})
        except Exception as e:
            print(f"extract_source_info(): {e}")
            return {}

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

    def process_date_string(date_string):
        return date_parser.parse(date_string).strftime('%Y-%m-%d %H:%M:%S')

    def process_site_code(site_code):
        if pd.notnull(site_code):
            return site_code

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

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()


    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # Make request to url and alter the state being requested
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
            response_json = response.json()
            value_json = response_json.get("value", {})
            time_series_json = value_json.get("timeSeries", {})

            data_gen = extract_data_generated_value(value_json=value_json)
            data_gen_processed = process_date_string(date_string=data_gen)

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

                gauge_objects_list.append(Gauge(state_abbrev=state_abbrev,
                                                site_name=site_name,
                                                site_code=site_code_processed,
                                                discharge=discharge,
                                                gauge_height=gauge_height,
                                                data_gen=data_gen_processed,
                                                collect_date=collected_date_processed))
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
    sql_insert_gen = sql_insert_generator(sql_values_list=sql_values_statements_list,
                                          step_increment=sql_insertion_step_increment,
                                          sql_insert_string=sql_insert_string)

    # Build the sql for updating the task tracker table for this process.
    sql_task_tracker_update = f"UPDATE RealTime_TaskTracking SET lastRun = '{start_date_time}', DataGenerated = (SELECT max(DataGenerated) from {database_table_name}) WHERE taskName = '{task_name}'"

    with pyodbc.connect(full_connection_string) as connection:
        cursor = connection.cursor()

        try:
            cursor.execute(sql_delete_string)
        except Exception as e:
            print(f"Error deleting records from {database_table_name}. {e}")
            exit()
        else:
            print(f"Delete statement executed. Time elapsed {time_elapsed(start=start)}")

        insert_round_count = 1
        for batch in sql_insert_gen:
            try:
                cursor.execute(batch)
            except pyodbc.DataError:
                print(f"A value in the sql exceeds the field length allowed in database table: {batch}")
            else:
                print(f"Executing insert batch {insert_round_count}. Time elapsed {time_elapsed(start=start)}")
                insert_round_count += 1
        try:
            cursor.execute(sql_task_tracker_update)
        except pyodbc.DataError:
            print(f"A value in the sql exceeds the field length allowed in database table: {sql_task_tracker_update}")

        connection.commit()
        print(f"Commit successful. Time elapsed {time_elapsed(start=start)}")

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