"""

"""


def main():
    # IMPORTS
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
    config_file = r"doit_config_WebEOCShelters.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    current_year = datetime.now().year
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    mema_cfg_section_name = "MEMA_VALUES"
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

    def extract_all_immediate_child_features_from_element(element: ET.Element, tag_name: str) -> list:
        """
        Extract all immediate children of the element provided to the method.

        :param element: ET.Element of interest to be interrogated
        :param tag_name: tag of interest on which to search
        :return: list of all discovered ET.Element items
        """
        try:
            result = element.findall(tag_name)
        except AttributeError as ae:
            print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
            exit()
        else:
            return result
        # else:
        #     if len(result) == 0:
        #         # NOTE: The 'r' in front of the url is essential for this to work.
        #         altered_tag_name = appended_unnecessary_url + tag_name
        #         # print(f"Altering...{altered_tag_name}")
        #         return element.findall(altered_tag_name)
        #     else:
        #         return result

        # try:
        #     return element.findall(tag_name)
        # except AttributeError as ae:
        #     print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
        #     exit()

    def extract_first_immediate_child_feature_from_element(element: ET.Element, tag_name: str) -> ET.Element:
        """Extract first immediate child feature from provided xml ET.Element based on provided tag name

        :param element: xml ET.Element to interrogate
        :param tag_name: name of desired tag
        :return: ET.Element of interest
        """

        try:
            result = element.find(tag_name)
        except AttributeError as ae:
            print(f"AttributeError: Unable to extract '{tag_name}' from {element.text}: {ae}")
            exit()
        else:
            return result

    def handle_tag_name_excess(xml_extraction_func, element: ET.Element, tag_name: str):
        """
        Use regular expressions to search tag names, containing prepended junk, for desired value at end of tag string.
        A decorator was not used because I had control over the original function and wasn't working with a
        pre-existing function whose behavior I could not alter. I thought the decorator would be less obvious.

        :param xml_extraction_func: doit xml extraction function needed for current tag name search
        :param element: xml element
        :param tag_name: string tag name being sought
        :return: None or value that extraction func returns
        """
        re_pattern = f"{tag_name}$"  # Junk always begins string and tag is at the end. Money sign indicates search end.
        for item in element:
            result = re.search(pattern=re_pattern, string=item.tag)
            if result:
                return xml_extraction_func(element=element, tag_name=item.tag)
            else:
                continue
        return None

    def parse_xml_response_to_element(response_xml_str: str) -> ET.Element:
        """
        Process xml response content to xml ET.Element
        :param response_xml_str: string xml from response
        :return: xml ET.Element
        """
        try:
            return ET.fromstring(response_xml_str)
        except Exception as e:  # TODO: Improve exception handling
            print(f"Unable to process xml response to Element using ET.fromstring(): {e}")
            exit()

    def process_date_string(date_string: str) -> str:
        """
        Parse the date string to datetime format using the dateutil parser and return string formatted
        Old CGIS way was to manipulate string by removing a 'T' and doing other actions instead of using module
        :param date_string: string extracted from response json
        :return: date/time string formatted as indicated
        """
        try:
            return date_parser.parse(date_string).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            return '1970-01-01 00:00:00'

    def process_polygon_elem_result(poly_elem: ET.Element) -> str:
        """
          Process geometry value for entry into SQL database as WKT and return, or return "'Null'"
          if present, comes in as text like this '37.23,-89.59 37.25,-89.41 37.13,-89.29 37.09,-89.46 37.23,-89.59'
          CGIS code note said the following: need to convert polygon list to WKT and reverse lat long (CGIS)
          WKT appears to be "Well Known Text", has to do with database representation of coordinate
          reference systems
        :param poly_elem: geometry element
        :return: string for entry in database
        """
        if poly_elem.text is None:
            return "'Null'"  # Appears that database requires Null and not nan or other entry when no geometry
        else:
            poly_values = poly_elem.text
            coord_pairs_list = poly_values.split(" ")
            coord_pairs_list_switched = [f"""{value.split(',')[1]} {value.split(',')[0]}""" for value in
                                         coord_pairs_list]
            coords_for_database_use = ",".join(coord_pairs_list_switched)
            result = """geometry::STGeomFromText('POLYGON(({coords_joined}))', 4326)""".format(
                coords_joined=coords_for_database_use)
            return result

    def replace_problematic_chars_w_underscore(string: str) -> str:
        """
        Replace illegal/problematic characters text to avoid database issues and return cleaned string.
        Apostrophes were showing in county names and this was causing database insert issues. Designed for
        potential to expand the characters that are replaced.
        :param string: text to be evaluated
        :return: cleaned text
        """
        problem_characters = ("'",)
        for char in problem_characters:
            string = string.replace(char, "_")
        return string

    def setup_config(cfg_file: str) -> configparser.ConfigParser:
        """
        Instantiate the parser for accessing a config file.
        :param cfg_file: config file to access
        :return:
        """
        cfg_parser = configparser.ConfigParser()
        cfg_parser.read(filenames=cfg_file)
        return cfg_parser

    # CLASSES

    # FUNCTIONALITY
    start = datetime.now()
    print(f"Process started: {start}")

    # When using a DEV & PROD file during the redesign, avoid issues in using wrong database by inspecting script name.
    # FIXME: turn this back on when move to server
    # database_cfg_section_name = determine_database_config_value_based_on_script_name()

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    # need mema specific values for post requests
    mema_request_header_dict = json.loads(config_parser[mema_cfg_section_name]["HEADER"])
    mema_request_url = config_parser[mema_cfg_section_name]["URL"]
    mema_request_xml_data_template = config_parser[mema_cfg_section_name]["XML_DATA_TEMPLATE"]
    mema_request_password = config_parser[mema_cfg_section_name]["PASSWORD"]
    mema_request_username = config_parser[mema_cfg_section_name]["USERNAME"]
    xml_body_string = mema_request_xml_data_template.format(username=mema_request_username,
                                                            password=mema_request_password,
                                                            year_value=current_year)

    # need to make requests to mema url to get xml for interrogation and data extraction
    response = requests.post(url=mema_request_url, data=xml_body_string, headers=mema_request_header_dict)
    xml_response_root = parse_xml_response_to_element(response_xml_str=response.text)


    return


if __name__ == "__main__":
    main()
