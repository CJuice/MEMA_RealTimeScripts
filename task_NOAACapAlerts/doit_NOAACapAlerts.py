"""

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
    import time

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_NOAACapAlerts.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"

    # ASSERTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

    # CLASSES
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
        Inspect the python script file name to see if it includes _DEV, _PROD, or neither and return appropriate value.
        During redesign there was a DEV and PROD version and each wrote to a different database. When manually
        deploying there was opportunity to error because the variable value had to be manually switched. Now all that
        has to happen is the file name has to be switched and the correct config file section is accessed.
        :return: string value for config file section to be accessed for database identity
        """

        file_name, extension = os.path.splitext(os.path.basename(__file__))
        if "_DEV" in file_name:
            return "DATABASE_DEV"
        elif "_PROD" in file_name:
            return "DATABASE_PROD"
        else:
            print(f"Script name does not contain _DEV or _PROD so proper Datbase config file section undetected")
            exit()

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
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    return


if __name__ == "__main__":
    main()
