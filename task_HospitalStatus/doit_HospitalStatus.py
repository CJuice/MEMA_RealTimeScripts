"""
This is a procedural script for populating MEMA database with hospital status data.

This process accesses three “CHATS Region County/Hospital Alert Tracking System” html pages containing tables
on the status of hospitals in a few different areas of interest to emergency management. The process pulls the
hospitals table into a python pandas dataframe and processes the data into SQL statements. The SQL statements
are used to insert the table data into a SQL table tracking the most data as of the last process. The SQL table
is accessed by the OSPREY Dashboard and influences the results in the hospitals row.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190327
Revisions: 20190329, CJuice: Sometimes the html page returned to the request does not contain the
hospitals html table. Sometimes the html page contains the hospitals table but it is empty. We were getting
failures every few hours due to these issues. On one run, a PROD task ran with issues and literally 2 seconds
later an identical DEV task ran without issue, requesting from the same urls. Redesigned to have a while loop
with three attempts each separated by a 5 second sleep. If don't succeed in three attempts then exit.
20190405, CJuice: After a mistake in deploying where the database config file section was not switched from DEV to
PROD, and the wrong database was being written to, I added a function to detect DEV or PROD in the script name and
return the appropriate value based off of that naming convention.
"""


def main():
    print(f"main() entered.")

    # IMPORTS
    from datetime import datetime
    import configparser
    import numpy as np
    import os
    import pandas as pd
    import pyodbc
    import requests
    import time
    print(f"Imports completed.")

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_HospitalStatus.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    delay_seconds = 2
    html_id_hospital_table = "tblHospitals"
    realtime_hospitalstatus_headers = (
    "Linkname", "Status", "Yellow", "Red", "Mini", "ReRoute", "t_bypass", "DataGenerated")
    realtime_hospstat_tbl = "[{database_name}].[dbo].[RealTime_HospitalStatus]"
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{hospital}', '{status_level_value}', '{yellow_alert}', '{red_alert}', '{mini_disaster}', '{reroute}', '{trauma_bypass}', '{created_date_string}'"""
    task_name = "HospitalStatus"
    urls_list = ["https://www.miemssalert.com/chats/Default.aspx?hdRegion=3",
                 "https://www.miemssalert.com/chats/Default.aspx?hdRegion=124",
                 "https://www.miemssalert.com/chats/Default.aspx?hdRegion=5"]
    print(f"Variables completed.")

    # ASSERT STATEMENTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

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

    def determine_status_level(html_row_series: pd.Series):
        """
        Evaluate presence of data in html table and return string based on business logic tree.

        This is reproduced functionality from interpretation of single line statement in old code
        that determined the 'Status' value in database table
        OLD PYTHON STATEMENT:
            "red" if row[4] is not '' else "yellow" if row[3] is not '' or row[6] is not ''
            else "t_bypass" if row[7] is not '' else "mini" if row[5] is not '' else "normal"
        'row' was a record from an html table, with two values appended at the beginning. The old process was
        basically looking for a value other than null/empty, and there is a hierarchy of importance if values
        are simultaneously present. The old way created a list called 'row' that started with created date and
        current date, then the row contents from html table. Redesign subtracts two from old index positions
        since the two date values are no longer a factor.

        :param html_row_series: pandas series containing data from a row of html table
        :return:
        """
        # Get the values in the table or a default of numpy NaN
        yellow_alert_ser_val = html_row_series.get(key="Yellow Alert", default=np.NaN)
        red_alert_ser_val = html_row_series.get(key="Red Alert", default=np.NaN)
        mini_disaster_ser_val = html_row_series.get(key="Mini Disaster", default=np.NaN)
        reroute_ser_val = html_row_series.get(key="ReRoute", default=np.NaN)
        trauma_bypass_ser_val = html_row_series.get(key="Trauma ByPass", default=np.NaN)

        # check for presence of any non-null, value in order of business importance level, and return result
        if pd.notnull(red_alert_ser_val):
            # Red alerts are top priority
            return "red"
        else:
            if pd.notnull(yellow_alert_ser_val) or pd.notnull(reroute_ser_val):
                # Yellow or ReRoute take second priority
                return "yellow"
            else:
                if pd.notnull(trauma_bypass_ser_val):
                    # Trauma ByPass is third
                    return "t_bypass"
                else:
                    if pd.notnull(mini_disaster_ser_val):
                        # Mini Disaster is fourth
                        return "mini"
                    else:
                        return "normal"

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

    print(f"Functions completed.")

    # FUNCTIONALITY
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # need to get data, parse data, process data for each url in the list
    for url_index, url_string in enumerate(urls_list):

        # Due to known issues with html table presence and content, coding three potential attempts.
        round_count = 0
        problem_encountered = False
        while round_count < 4:
            round_count += 1
            print(f"\nHTML Table Issue Handling - Attempt Count: {round_count}")
            print(f"Time elapsed {time_elapsed(start=start)}")

            # Setup blank variables, in case of multiple tries
            response = None
            html_table_dfs_list = None
            html_table_df = None

            # Make request to url
            print(f"Making request to {url_string}")
            try:
                response = requests.get(url=url_string, params={})
            except Exception as e:
                print(f"Exception during request for html page {url_string}. {e}")
                exit(code=1)
            else:
                print(f"Response status code: {response.status_code}")

            # Need the html table in a readable format for use. Use a pandas dataframe.
            try:
                html_table_dfs_list = pd.read_html(io=response.text, header=0, attrs={"id": html_id_hospital_table})
            except ValueError as ve:

                # Sometimes the web page does not contain a hospital table. No clue as to why but is temporary so retry.
                print(ve)
                print(f"Value Error: No tables found in html page. Expected 1 table with id = {html_id_hospital_table}")
                print(f"WebPage: {url_string}, Response status code: {response.status_code}")
                print(f"Sleeping for {delay_seconds} seconds...")

                # Try another round, after small delay
                time.sleep(delay_seconds)
                problem_encountered = True
                continue
            else:
                # Html table of interest must have been present so move on.
                pass

            # Need to get the hospitals table by the html id for the table. HTML id's are unique so should be 1 table.
            try:
                html_table_df = html_table_dfs_list[0]  # html id's are unique so should only be 1 item in list
            except IndexError as ie:

                # Sometimes the web page contains an empty hospital table. No clue why but is temporary so retry.
                print(ie)
                print(f"Index Error: length of html_table_dfs_list is {len(html_table_dfs_list)}; Expected length = 1.")
                print(f"WebPage where issue was encountered: {url_string}, Response status code: {response.status_code}")
                print(f"Sleeping for {delay_seconds} seconds...")
                time.sleep(delay_seconds)
                problem_encountered = True
                continue
            else:
                # Html table of interest must have content so break out of while loop.
                break

        # Check to see if used up the 3 attempts and if a problem was encountered on third attempt.
        if round_count == 3 and problem_encountered:
            print(f"Could not resolve issues with HTML in 3 rounds of attempts with {delay_seconds} second sleeps.")
            print("Exiting")
            print(f"Time elapsed {time_elapsed(start=start)}")
            exit(code=1)

        # Need an iteration to provide rows from the dataframe.
        row_generator = html_table_df.iterrows()
        for row_index, row_series in row_generator:
            status_level_value = determine_status_level(html_row_series=row_series)
            hospital, yellow_alert, red_alert, mini_disaster, reroute, trauma_bypass, *rest = row_series
            values = sql_values_string_template.format(hospital=hospital,
                                                       status_level_value=status_level_value,
                                                       yellow_alert=yellow_alert,
                                                       red_alert=red_alert,
                                                       mini_disaster=mini_disaster,
                                                       reroute=reroute,
                                                       trauma_bypass=trauma_bypass,
                                                       created_date_string=start_date_time)
            values_string = sql_values_statement.format(values=values)
            sql_values_statements_list.append(values_string)

    # Database Transactions
    print("\nDatabase operations initiated...")
    print(f"Time elapsed {time_elapsed(start=start)}")
    database_name = config_parser[database_cfg_section_name]["NAME"]
    database_password = config_parser[database_cfg_section_name]["PASSWORD"]
    database_user = config_parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)
    realtime_hopstat_tbl_string = realtime_hospstat_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_hospitalstatus_headers])
    sql_delete_insert_string = sql_delete_insert_template.format(
        table=realtime_hopstat_tbl_string,
        headers_joined=headers_joined)

    # Build the entire SQL statement to be executed
    full_sql_string = sql_delete_insert_string + ",".join(sql_values_statements_list)

    # Build the sql for updating the task tracker table for this process.
    sql_task_tracker_update = f"UPDATE RealTime_TaskTracking SET lastRun = '{start_date_time}', DataGenerated = (SELECT max(DataGenerated) from {realtime_hopstat_tbl_string}) WHERE taskName = '{task_name}'"

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
