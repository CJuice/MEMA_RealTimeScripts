"""
This is a procedural script for populating MEMA database with Shelter data.

This process makes request to MEMA web services. It captures many values from response JSON. See the Shelter dataclass
for insights into the values that are extracted. Shelter Dataclass objects are created with these values and
stored in a list. The list of objects is accessed and used to generate the values in the insert sql statement.
Once the insert statement is completed a database connection is established, all existing records are deleted,
and the new records are inserted.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190418
Revisions:
"""


def main():

    # IMPORTS
    from dataclasses import dataclass
    from datetime import datetime
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
        # Notice, Main and Secondary are not in the headers list or template string for values
    realtime_webeocshelters_headers = ('TableName', 'DataID', 'UserName', 'PositionName', 'EntryDate',
                                       'ShelterTier', 'ShelterType', 'ShelterName',
                                       'ShelterAddress', 'OwnerTitle', 'OwnerContact', 'OwnerContactNumber',
                                       'FacContactTitle', 'FacContactName', 'FacContactNumber', 'County',
                                       'ShelterStatus', 'Capacity', 'Occupancy', 'Arc', 'SpecialNeeds',
                                       'PetFriendly', 'Generator', 'FuelSource', 'ExoticPet', 'IndoorHouse',
                                       'Geometry', 'DataGenerated', 'remove')
    realtime_webeocshelters_tbl = "[{database_name}].[dbo].[RealTime_WebEOCShelters]"
    shelter_objects_list = []
    sql_delete_insert_template = """DELETE FROM {table}; INSERT INTO {table} ({headers_joined}) VALUES """
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{table_name}', {data_id}, '{user_name}', '{position_name}', '{entry_date}', '{shelter_tier}', '{shelter_type}', '{shelter_name}', '{shelter_address}', '{owner_title}', '{owner_contact}', '{owner_contact_number}', '{fac_contact_title}', '{fac_contact_name}', '{fac_contact_number}', '{county}', '{shelter_status}', {capacity}, {occupancy}, '{arc}', '{special_needs}', '{pet_friendly}', '{generator}', '{fuel_source}', '{exotic_pet}', '{indoor_house}', {geometry}, '{data_gen}', {remove}"""
    task_name = "WebEOCShelters"

    print(f"Variables completed.")

    # ASSERTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

    # FUNCTIONS
    def clean_record_string_values_for_database(value_dict: dict) -> dict:
        """
        Clean string values by stripping whitespace from values.
        Noticed some values have extra whitespace or values are just whitespace (address is example)
        example of this is address value of '  '
        :param value_dict: record dictionary extraced from xml
        :return: value dict with cleaned values
        """
        for key, value in value_dict.items():
            try:
                value_dict[key] = value.strip()
            except AttributeError as ae:
                pass
        return value_dict

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

    def process_geometry_value(geometry_value: str) -> str:
        """
        Process geometry value for entry into SQL database, or return "'Null'"
        if present, substitute into the correct sql string format for a geometry value and if not, return 'Null'
        NOTE: SQL insertion syntax for geometry requires the following syntax (example):
            geometry::STGeomFromText('POINT (-76.8705880274927 38.9963106309707)', 4326)
        The geometry::STGeomFromText portion and spatial ref cannot be surrounded by single/double quotes
        as it is a sql action not a string.
        :param geometry_value: geometry value extracted from xml
        :return: string for entry in database
        """
        if geometry_value == "" or geometry_value == "'Null'":
            return "'Null'"  # Appears that database requires 'Null' and not nan or other entry when no geometry
        else:
            result = """geometry::STGeomFromText('{geometry_value}', 4326)""".format(
                geometry_value=geometry_value)
            return result

    def process_remove_value(remove_value: str) -> int:
        """
        Need to convert the remove value for a True/False equivalent
        :param remove_value: string value extracted from xml
        :return: integer for true (1), false (0)
        """
        if remove_value.lower() == "yes":
            return 1
        else:
            return 0

    def process_user_name(value: str) -> str:
        """
        Logic for processing extracted user name value and returning original value or substitute string
        :param value: extracted value
        :return: string substitute or original value
        """
        if value == "":
            return "User Account No Longer Exists"
        else:
            return value

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

    def time_elapsed(start=datetime.now()):
        """
        Calculate the difference between datetime.now() value and a start datetime value
        :param start: datetime value
        :return: datetime.timedelta value
        """
        return datetime.now() - start

    # CLASSES
    @dataclass
    class Shelter:
        """Dataclass to hold all extracted variables needed for later sql table insertion"""
        table_name: str
        data_id: int
        user_name: str
        position_name: str
        entry_date: str
        shelter_tier: str
        shelter_type: str
        name: str
        address: str
        owner_title: str
        owner_contact: str
        owner_contact_number: str
        fac_contact_title: str
        fac_contact_name: str
        fac_contact_number: str
        county: str
        status: str
        eva_capacity: str
        eva_occupancy: str
        arc: str
        special_needs: str
        pet_friendly: str
        generator: str
        fuel_source: str
        exotic_pet: str
        indoor_house: str
        geometry: str
        remove: int
        data_gen: str

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

    # NOTE: burrowing down into the xml step by step
    body_element = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                          element=xml_response_root,
                                          tag_name="Body")
    data_response_element = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                   element=body_element,
                                                   tag_name="GetDataResponse")
    data_result_element = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                 element=data_response_element,
                                                 tag_name="GetDataResult")

    # NOTE: For some reason the content of the data_result_element is not recognized as xml, but able to parse to xml
    data_element = parse_xml_response_to_element(response_xml_str=data_result_element.text)
    record_elements = handle_tag_name_excess(xml_extraction_func=extract_all_immediate_child_features_from_element,
                                             element=data_element,
                                             tag_name="record")

    for record in record_elements:
        record_dict = clean_record_string_values_for_database(record.attrib)

        # need to handle the few items that require processing for data type or logic
        data_id = int(record_dict.get("dataid", -9999))
        user_name = process_user_name(record_dict.get("username", np.NaN))
        name = replace_problematic_chars_w_underscore(record_dict.get("name", np.NaN))
        address = replace_problematic_chars_w_underscore(record_dict.get("address", np.NaN))
        county = replace_problematic_chars_w_underscore(record_dict.get("county", np.NaN))
        geometry = process_geometry_value(record_dict.get("theGeometry", "'Null'"))
        remove = process_remove_value(record_dict.get("remove", 0))

        # Create and store the shelter dataclass objects for database action use.
        shelter_objects_list.append(Shelter(table_name=record_dict.get("tablename", np.NaN),
                                            data_id=data_id,
                                            user_name=user_name,
                                            position_name=record_dict.get("positionname", np.NaN),
                                            entry_date=record_dict.get("entrydate", np.NaN),
                                            shelter_tier=record_dict.get("shelterTier", np.NaN),
                                            shelter_type=record_dict.get("shelterType", np.NaN),
                                            name=name,
                                            address=address,
                                            owner_title=record_dict.get("ownertitle", np.NaN),
                                            owner_contact=record_dict.get("ownercontact", np.NaN),
                                            owner_contact_number=record_dict.get("ownercontactnumber", np.NaN),
                                            fac_contact_title=record_dict.get("fac_contact_title", np.NaN),
                                            fac_contact_name=record_dict.get("fac_contactname", np.NaN),
                                            fac_contact_number=record_dict.get("fac_contactnumber", np.NaN),
                                            county=county,
                                            status=record_dict.get("status", np.NaN),
                                            eva_capacity=record_dict.get("eva_capacity", np.NaN),
                                            eva_occupancy=record_dict.get("eva_occupancy", np.NaN),
                                            arc=record_dict.get("arc", np.NaN),
                                            special_needs=record_dict.get("specialneeds", np.NaN),
                                            pet_friendly=record_dict.get("petfriendly", np.NaN),
                                            generator=record_dict.get("Generator", np.NaN),
                                            fuel_source=record_dict.get("fuel_source", np.NaN),
                                            exotic_pet=record_dict.get("exoticpet", np.NaN),
                                            indoor_house=record_dict.get("indoorhouse", np.NaN),
                                            geometry=geometry,
                                            remove=remove,
                                            data_gen=start_date_time)
                                    )

    # Need to build out and store the VALUES for sql table insertion
    for shelter in shelter_objects_list:
        values = sql_values_string_template.format(table_name=shelter.table_name,
                                                   data_id=shelter.data_id,
                                                   user_name=shelter.user_name,
                                                   position_name=shelter.position_name,
                                                   entry_date=shelter.entry_date,
                                                   shelter_tier=shelter.shelter_tier,
                                                   shelter_type=shelter.shelter_type,
                                                   shelter_name=shelter.name,
                                                   shelter_address=shelter.address,
                                                   owner_title=shelter.owner_title,
                                                   owner_contact=shelter.owner_contact,
                                                   owner_contact_number=shelter.owner_contact_number,
                                                   fac_contact_title=shelter.fac_contact_title,
                                                   fac_contact_name=shelter.fac_contact_name,
                                                   fac_contact_number=shelter.fac_contact_number,
                                                   county=shelter.county,
                                                   shelter_status=shelter.status,
                                                   capacity=shelter.eva_capacity,
                                                   occupancy=shelter.eva_occupancy,
                                                   arc=shelter.arc,
                                                   special_needs=shelter.special_needs,
                                                   pet_friendly=shelter.pet_friendly,
                                                   generator=shelter.generator,
                                                   fuel_source=shelter.fuel_source,
                                                   exotic_pet=shelter.exotic_pet,
                                                   indoor_house=shelter.indoor_house,
                                                   geometry=shelter.geometry,
                                                   data_gen=shelter.data_gen,
                                                   remove=shelter.remove)
        values_string = sql_values_statement.format(values=values)
        sql_values_statements_list.append(values_string)

    print(f"Requests, data capture, and processing completed. Time elapsed {time_elapsed(start=start)}")

    # Database Transactions
    print(f"Database operations initiated. Time elapsed {time_elapsed(start=start)}")
    database_name = config_parser[database_cfg_section_name]["NAME"]
    database_password = config_parser[database_cfg_section_name]["PASSWORD"]
    database_user = config_parser[database_cfg_section_name]["USER"]
    full_connection_string = create_database_connection_string(db_name=database_name,
                                                               db_user=database_user,
                                                               db_password=database_password)
    realtime_webeocshelters_tbl_string = realtime_webeocshelters_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the DELETE & INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_webeocshelters_headers])
    sql_delete_insert_string = sql_delete_insert_template.format(
        table=realtime_webeocshelters_tbl_string,
        headers_joined=headers_joined)

    # Build the entire SQL statement to be executed
    full_sql_string = sql_delete_insert_string + ",".join(sql_values_statements_list)

    # Build the sql for updating the task tracker table for this process.
    sql_task_tracker_update = f"UPDATE RealTime_TaskTracking SET lastRun = '{start_date_time}', DataGenerated = (SELECT max(DataGenerated) from {realtime_webeocshelters_tbl_string}) WHERE taskName = '{task_name}'"

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
