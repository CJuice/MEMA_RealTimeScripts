"""

"""


def main():
    # IMPORTS
    from datetime import datetime
    import configparser
    import numpy as np
    import os
    import pyodbc
    import requests

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    config_file = r"doit_config_WebEOCShelters.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    realtime_webeocshelters_headers = ('ID', 'TableName', 'DataID', 'UserName', 'PositionName', 'EntryDate',
                                       'Main', 'Secondary', 'ShelterTier', 'ShelterType', 'ShelterName',
                                       'ShelterAddress', 'OwnerTitle', 'OwnerContact', 'OwnerContactNumber',
                                       'FacContactTitle', 'FacContactName', 'FacContactNumber', 'County',
                                       'ShelterStatus', 'Capacity', 'Occupancy', 'Arc', 'SpecialNeeds',
                                       'PetFriendly', 'Generator', 'FuelSource', 'ExoticPet', 'IndoorHouse',
                                       'Geometry', 'DataGenerated', 'remove')
    realtime_webeocshelters_tbl = "[{database_name}].[dbo].[RealTime_WebEOCShelters]"
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """"""
    task_name = "WebEOCShelters"

    print(f"Variables completed.")

    # ASSERTS
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
            print(f"Script name does not contain _DEV or _PROD so proper Database config file section undetected")
            exit()

    # CLASSES

    # FUNCTIONALITY

    return


if __name__ == "__main__":
    main()
