"""
This is a procedural script for populating MEMA database with NOAA CAP Alerts data.

This process makes requests to NOAA urls for Common Alerting Protocol (CAP) alerts. It uses fips codes to form the
urls, makes the requests, and receives an xml response. The response is processed for values of interest.
These values are a title, a link, date published, date updated, summary, date effective, date expires, status,
message type, urgency, severity, certainty, area description, tips code, event category, geometry if present,
and a date generated value. The values extracted are encapsulated in a dataclass object that is stored in a list.
The list of objects is accessed and used to generate the values in the insert sql statement.
Once the insert statement is completed a database connection is established, all existing records are deleted,
and the new records are inserted.
Redesigned from the original CGIS version when MEMA server environments were being migrated to new versions.
Author: CJuice, 20190415
Revisions:
"""


def main():

    # IMPORTS
    from datetime import datetime
    from dateutil import parser as date_parser
    import configparser
    from dataclasses import dataclass
    import numpy as np
    import os
    import pyodbc
    import re
    import requests
    import xml.etree.ElementTree as ET

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
    alert_objects = []
    config_file = r"doit_config_NOAACapAlerts.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    mdc_code_template = "MDC{fips_last_three}"
    noaa_fips_values = [24001, 24003, 24005, 24510, 24009, 24011, 24013, 24015, 24017, 24019, 24021, 24023, 24025,
                        24027, 24029, 24031, 24033, 24035, 24037, 24039, 24041, 24043, 24045, 24047]
    noaa_url_template = r"""http://alerts.weather.gov/cap/wwaatmget.php?x={code}&y=0"""
    realtime_noaacapalerts_headers = ('AlertText', 'URL', 'PublishDate', 'LastUpdated', 'Summary',
                                      'EffectiveDate', 'ExpirationDate', 'Status', 'Type', 'Urgency', 'Severity',
                                      'Certainty', 'County', 'fips', 'Event', 'geometry', 'DataGenerated')
    realtime_noaacapalerts_tbl = "[{database_name}].[dbo].[RealTime_NOAACapALerts]"
    sql_delete_template = """DELETE FROM {table};"""
    sql_insert_template = """INSERT INTO {table} ({headers_joined}) VALUES """
    sql_insertion_step_increment = 1000
    sql_values_statement = """({values})"""
    sql_values_statements_list = []
    sql_values_string_template = """'{title}', '{link}', '{published}', '{updated}', '{summary}', '{cap_effective}', '{cap_expires}', '{cap_status}', '{cap_msg_type}', '{cap_urgency}', '{cap_severity}', '{cap_certainty}', '{cap_area_desc}', '{fips}', '{cap_event}', {cap_geometry}, '{data_gen}'"""  # Removed ID field
    task_name = "NOAACapAlerts"

    # ASSERTS
    assert os.path.exists(config_file_path)
    print("Assertion tests completed.")

    # CLASSES
    @dataclass
    class CAPEntry:
        """Data class for holding essential values about an alert; most values inserted into SQL database"""
        cap_area_desc: str = np.NaN
        cap_certainty: str = np.NaN
        cap_effective: str = np.NaN
        cap_event: str = np.NaN
        cap_expires: str = np.NaN
        cap_msg_type: str = np.NaN
        cap_polygon: str = "'Null'"  # Appears that it must be 'Null' and not 'nan'
        cap_severity: str = np.NaN
        cap_status: str = np.NaN
        cap_urgency: str = np.NaN
        data_gen: str = '1970-01-01 00:00:00'
        fips: str = np.NaN
        link: str = np.NaN
        published: str = '1970-01-01 00:00:00'
        summary: str = np.NaN
        title: str = np.NaN
        updated: str = '1970-01-01 00:00:00'

    # FUNCTIONS
    def assemble_fips_to_mdccode_dict(url_template: str, mdc_code_template: str, fips_values: list) -> dict:
        """
        Create NOAA Cap Alert urls from fips codes and return a dictionary of fips keys and url values.
        A valid url uses an MDC code which appears to be the letters MDC and the last three numbers of a fips code.
        The existing CGIS process contained a list of fips code that were taken to be those of interest to the process.
        Each of the fips codes is converted to string, the last three digits are extracted, appended to the end of 'MDC'
        and then substituted into a template url string. The fips code is then used as a dictionary key and the url
        becomes the dictionary value. This dictionary is returned for use.
        :param url_template: string template for NOAA Cap Alerts urls
        :param mdc_code_template: string template similar to 'MDC---' where the three dashes are to be numbers from fips
        :param fips_values: list of fips values of interest for the process
        :return: dictionary of string fips keys and NOAA Cap Alert url values
        """
        output_dict = {}
        for value in fips_values:
            value = str(value)
            last_three = value[2:]
            mdc_code = mdc_code_template.format(fips_last_three=last_three)
            full_url = url_template.format(code=mdc_code)
            output_dict[value] = full_url
        return output_dict

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
        NOTE: SQL insertion syntax for geometry requires the following syntax (example):
        geometry::STGeomFromText('POINT (-76.8705880274927 38.9963106309707)', 4326)
        The geometry::STGeomFromText portion and spatial ref cannot be surrounded by single/double quotes
        as it is a sql action not a string.
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

    def sql_insert_generator(sql_values_list: list, step_increment: int, sql_insert_string: str):
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
        :return: datetime.timedelta value
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

    # need a dictionary with fips code keys and urls for requests
    noaa_cap_alerts_urls_dict = assemble_fips_to_mdccode_dict(url_template=noaa_url_template,
                                                              mdc_code_template=mdc_code_template,
                                                              fips_values=noaa_fips_values)

    # need to make requests to noaa urls to get xml for interrogation and data extraction
    for fips, noaa_cap_alert_url in noaa_cap_alerts_urls_dict.items():
        response = requests.get(url=noaa_cap_alert_url)
        xml_response_root = parse_xml_response_to_element(response_xml_str=response.text)
        entry_element = handle_tag_name_excess(xml_extraction_func=extract_all_immediate_child_features_from_element,
                                               element=xml_response_root,
                                               tag_name="entry")
        doc_updated_element = handle_tag_name_excess(
            xml_extraction_func=extract_first_immediate_child_feature_from_element,
            element=xml_response_root,
            tag_name="updated")

        # ignored time zone and dst etc conversions at time of redesign. Possible TODO
        date_updated = process_date_string(date_string=doc_updated_element.text)

        # Extract values of interest from the entry element that was extracted in a previous step
        for data in entry_element:
            title_text = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                element=data,
                                                tag_name="title").text
            title_text_processed = replace_problematic_chars_w_underscore(string=title_text)

            print(f"{fips}: {title_text_processed}")
            if title_text_processed == "There are no active watches, warnings or advisories":
                alert_objects.append(CAPEntry(data_gen=date_updated, fips=fips, title=title_text_processed))
                break
            else:
                cap_area_desc = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="areaDesc").text
                cap_area_desc_processed = replace_problematic_chars_w_underscore(string=cap_area_desc)

                cap_certainty = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="certainty").text

                cap_effective = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="effective").text
                cap_effective_processed = process_date_string(date_string=cap_effective)

                cap_event = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="event").text

                cap_expires = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="expires").text
                cap_expires_processed = process_date_string(date_string=cap_expires)

                cap_msg_type = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="msgType").text

                cap_polygon_elem = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="polygon")
                cap_polygon = process_polygon_elem_result(poly_elem=cap_polygon_elem)

                cap_severity = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="severity").text

                cap_status = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="status").text

                cap_urgency = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="urgency").text

                link = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                              element=data,
                                              tag_name="link").attrib.get("href", np.NaN)

                published = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="published").text
                published_processed = process_date_string(date_string=published)

                summary = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                 element=data,
                                                 tag_name="summary").text
                summary_processed = replace_problematic_chars_w_underscore(string=summary)

                updated = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                 element=data,
                                                 tag_name="updated").text
                updated_processed = process_date_string(date_string=updated)

                # Create CAPEntry dataclass objects and store for use in SQL VALUES building for INSERT statement
                alert_objects.append(CAPEntry(cap_area_desc=cap_area_desc_processed,
                                              cap_certainty=cap_certainty,
                                              cap_effective=cap_effective_processed,
                                              cap_event=cap_event,
                                              cap_expires=cap_expires_processed,
                                              cap_msg_type=cap_msg_type,
                                              cap_polygon=cap_polygon,
                                              cap_severity=cap_severity,
                                              cap_status=cap_status,
                                              cap_urgency=cap_urgency,
                                              data_gen=date_updated,
                                              fips=fips,
                                              link=link,
                                              published=published_processed,
                                              summary=summary_processed,
                                              title=title_text_processed,
                                              updated=updated_processed)
                                     )
    print(f"Requests, data capture, and processing completed. Time elapsed {time_elapsed(start=start)}")

    # Need to build the values string statements for use later on with SQL INSERT statement.
    for alert_obj in alert_objects:
        values = sql_values_string_template.format(title=alert_obj.title,
                                                   link=alert_obj.link,
                                                   published=alert_obj.published,
                                                   updated=alert_obj.updated,
                                                   summary=alert_obj.summary,
                                                   cap_effective=alert_obj.cap_effective,
                                                   cap_expires=alert_obj.cap_expires,
                                                   cap_status=alert_obj.cap_status,
                                                   cap_msg_type=alert_obj.cap_msg_type,
                                                   cap_urgency=alert_obj.cap_urgency,
                                                   cap_severity=alert_obj.cap_severity,
                                                   cap_certainty=alert_obj.cap_certainty,
                                                   cap_area_desc=alert_obj.cap_area_desc,
                                                   fips=alert_obj.fips,
                                                   cap_event=alert_obj.cap_event,
                                                   cap_geometry=alert_obj.cap_polygon,
                                                   data_gen=alert_obj.data_gen)
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
    database_table_name = realtime_noaacapalerts_tbl.format(database_name=database_name)

    # need the sql table headers as comma separated string values for use in the INSERT statement
    headers_joined = ",".join([f"{val}" for val in realtime_noaacapalerts_headers])
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

        # Due to 1000 record insert limit, delete records first and then do insertion rounds for alerts.
        # The quantity of alerts can vary in size, assuming this is why the old CGIS process accounted for potential
        #   insert quantity in excess of 1000 record sql limit. Generally seems to be very few records but doesn't hurt.
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
                print(f"A value in the sql exceeds the field length allowed in database table.\n{batch}\n")
                exit()
            except pyodbc.Error:
                print(f"pyodbc.Error raised while inserting records.\n{batch}\n")
                exit()
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


if __name__ == "__main__":
    main()
