"""
This is a procedural script for populating MEMA database with hospital status data.

This process accesses three “CHATS Region County/Hospital Alert Tracking System” html pages containing tables
on the status of hospitals in a few different areas of interest to emergency management. The process pulls the
hospitals table into a python pandas dataframe and processes the data into SQL statements. The SQL statements
are used to insert the table data into a SQL table tracking the most data as of the last process. The SQL table
is accessed by the OSPREY Dashboard and influences the results in the hospitals row.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190327

"""


def main():

    # IMPORTS
    from datetime import datetime
    import configparser
    import numpy as np
    import os
    import pandas as pd
    import pyodbc
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file_path = r"doit_config_HospitalStatus.cfg"
    database_cfg_section_name = "DATABASE"
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    html_id_hospital_table = "tblHospitals"
    realtime_hospitalstatus_headers = (
    "Linkname", "Status", "Yellow", "Red", "Mini", "ReRoute", "t_bypass", "DataGenerated")
    realtime_hospstat_tbl = "[OspreyDB_DEV].[dbo].[RealTime_HospitalStatus]"
    sql_delete_insert_template = """DELETE FROM {realtime_hospstat_tbl}; INSERT INTO {realtime_hospstat_tbl} ({headers_joined}) VALUES """
    sql_statements_list = []
    sql_values_statement = """({values})"""
    sql_values_string_template = """'{hospital}', '{status_level_value}', '{red_alert}','{yellow_alert}', '{mini_disaster}', '{reroute}', '{trauma_bypass}', '{created_date_string}'"""
    urls_list = ["https://www.miemssalert.com/chats/Default.aspx?hdRegion=3",
                 "https://www.miemssalert.com/chats/Default.aspx?hdRegion=124",
                 "https://www.miemssalert.com/chats/Default.aspx?hdRegion=5"]

    # ASSERT STATEMENTS
    assert os.path.exists(config_file_path)

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

    # def write_response_to_html(response_content: str, filename: str):
    #     """
    #     Write content to file.
    #     This was for writing the html pages to file. Unsure of reason this was done in old process. Recreated during
    #     development but decided to comment this out for production as the output html files do not appear to be
    #     consumed by any process or examined at any point by a person. The process was deemed wasteful and unnecessary
    #     and was dropped. The code remains in case a need is discovered.
    #
    #     :param response_content: Intended to be content of request response, html table in this process
    #     :param filename: name of the file to be written
    #     :return: none
    #     """
    #     with open(filename, "w") as handler:
    #         handler.write(response_content)
    #     return
    pass  # only here to get above commented out function to be collapsible and separate from FUNCTIONALITY

    # FUNCTIONALITY
    # need a current datetime stamp for database entry
    current_date_time = str(create_date_time_value())
    print(f"Process Date & Time: {current_date_time}")

    # need parser to access credentials
    parser = setup_config(config_file_path)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_hospitalstatus_headers])
    sql_delete_insert_string = sql_delete_insert_template.format(realtime_hospstat_tbl=realtime_hospstat_tbl,
                                                                 headers_joined=headers_joined)

    # need to get data, parse data, process data for each url in the list
    for url_index, url_string in enumerate(urls_list):
        print(f"Making request to {url_string}")
        output_filename_path = f"{_root_file_path}/data/HospitalStatus_{url_index}.html"

        # Make request to url
        try:
            response = requests.get(url=url_string, params={})
        except Exception as e:
            print(f"Exception during request for html page {url_string}. {e}")
            exit()

        # Old process wrote html page contents to file. Do not know how/if files are used. Preserving process.
        # try:
        # 	write_response_to_html(response_content=response.text, filename=output_filename_path)
        # except Exception as e:
        # 	print(f"Exception during writing of html file {output_filename_path}. {e}")
        # 	exit()
        # else:
        # 	print(f"HTML file written: {output_filename_path}")

        # Need the html table in a readable format for use. Pandas dataframe is cheap and easy.
        html_table_dfs_list = pd.read_html(io=response.text, header=0, attrs={"id": html_id_hospital_table})
        html_table_df = html_table_dfs_list[0]  # html id's are unique so should only be one item in list
        # print(html_table_df.info())

        # Need an iteration to provide rows from the dataframe.
        row_generator = html_table_df.iterrows()
        for row_index, row_series in row_generator:
            status_level_value = determine_status_level(html_row_series=row_series)
            hospital, yellow_alert, red_alert, mini_disaster, reroute, trauma_bypass, *rest = row_series
            values = sql_values_string_template.format(hospital=hospital,
                                                       status_level_value=status_level_value,
                                                       red_alert=red_alert,
                                                       yellow_alert=yellow_alert,
                                                       mini_disaster=mini_disaster,
                                                       reroute=reroute,
                                                       trauma_bypass=trauma_bypass,
                                                       created_date_string=current_date_time)
            values_string = sql_values_statement.format(values=values)
            sql_statements_list.append(values_string)
    full_sql_string = sql_delete_insert_string + ",".join(sql_statements_list)

    # Database Transactions
    print("Database operations initiated...")
    database_name = parser[database_cfg_section_name]["NAME"]
    database_password = parser[database_cfg_section_name]["PASSWORD"]
    database_user = parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)

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
