"""

"""


def main():
    # IMPORTS
    from datetime import datetime
    from dateutil import parser as date_parser
    import configparser
    from dataclasses import dataclass
    import numpy as np
    import os
    import pandas as pd
    import pyodbc
    import re
    import requests
    import time
    import xml.etree.ElementTree as ET
    import dateutil

    # VARIABLES
    _root_file_path = os.path.dirname(__file__)
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
    print(f"Assertion tests completed.")

    # CLASSES
    @dataclass
    class CAPEntry:
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
        data_gen: datetime = '1970-01-01 00:00:00'
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
        All of the tags in the root element begin with the string '{http://www.w3.org/2005/Atom}'. Unable to find
        the tag by it's name only. Chose to use a try with tag name and then fail to appended value
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
        # else:
        #     if result is None:
        #         # NOTE: The 'r' in front of the url is essential for this to work.
        #         altered_tag_name = appended_unnecessary_url + tag_name
        #         # print(f"Altering...{altered_tag_name}")
        #         return element.find(altered_tag_name)
        #     else:
        #         return result

    def for_testing_write_xml_to_file(fips, text):
        with open("test{fips}.txt".format(fips=fips), 'w') as handler:
            handler.write(text)

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
        re_pattern = f"{tag_name}$"
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

    def process_date_string(date_string):
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

    def process_polygon_elem_result(poly_elem):
        """
          TODO
          if present, comes in as text like this
          '37.23,-89.59 37.25,-89.41 37.13,-89.29 37.09,-89.46 37.23,-89.59'
          CGIS code note said the following: need to convert polygon list to WKT and reverse lat long (CGIS)
          WKT appears to be "Well Known Text", has to do with database representation of coordinate
          reference systems
        :param poly_elem: TODO
        :return: TODO
        """
        if poly_elem.text is None:
            # print(f"Problematic polygon element: {poly_elem.text}")
            # return np.NaN
            return "'Null'"
        else:
            values = poly_elem.text
            coord_pairs_list = values.split(" ")
            coord_pairs_list_switched = [f"""{value.split(',')[1]} {value.split(',')[0]}""" for value in
                                         coord_pairs_list]
            coords_for_database_use = ",".join(coord_pairs_list_switched)
            result = """geometry::STGeomFromText('POLYGON(({coords_joined}))', 4326)""".format(
                coords_joined=coords_for_database_use)
            return result

    def replace_problematic_chars_w_underscore(string: str) -> str:
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

    noaa_cap_alerts_urls_dict = assemble_fips_to_mdccode_dict(url_template=noaa_url_template,
                                                              mdc_code_template=mdc_code_template,
                                                              fips_values=noaa_fips_values)
    alert_objects = []
    for fips, noaa_cap_alert_url in noaa_cap_alerts_urls_dict.items():
        response = requests.get(url=noaa_cap_alert_url)

        # for_testing_write_xml_to_file(fips=fips, text=response.text) # TESTING

        xml_response_root = parse_xml_response_to_element(response_xml_str=response.text)
        entry_element = handle_tag_name_excess(xml_extraction_func=extract_all_immediate_child_features_from_element,
                                               element=xml_response_root,
                                               tag_name="entry")
        doc_updated_element = handle_tag_name_excess(
            xml_extraction_func=extract_first_immediate_child_feature_from_element,
            element=xml_response_root,
            tag_name="updated")

        # ignored time zone and dst etc conversions at this time
        date_updated = process_date_string(date_string=doc_updated_element.text)

        # continue
        for data in entry_element:
            title_text = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                element=data,
                                                tag_name="title").text
            title_text_processed = replace_problematic_chars_w_underscore(string=title_text)

            if title_text_processed == "There are no active watches, warnings or advisories":
                alert_objects.append(CAPEntry(data_gen=date_updated, fips=fips, title=title_text_processed))
                print(title_text_processed)
                break
            else:

                # TODO: Reorder value extraction to match CAPAlert object order, which is alphabetical
                link = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                              element=data,
                                              tag_name="link").attrib.get("href", np.NaN)

                published = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="published").text
                published_processed = process_date_string(date_string=published)

                updated = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                 element=data,
                                                 tag_name="updated").text
                updated_processed = process_date_string(date_string=updated)

                summary = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                 element=data,
                                                 tag_name="summary").text
                summary_processed = replace_problematic_chars_w_underscore(string=summary)

                cap_event = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="event").text

                cap_effective = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="effective").text
                cap_effective_processed = process_date_string(date_string=cap_effective)

                cap_expires = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="expires").text
                cap_expires_processed = process_date_string(date_string=cap_expires)

                cap_status = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="status").text
                cap_msg_type = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="msgType").text
                cap_urgency = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="urgency").text
                cap_severity = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="severity").text
                cap_certainty = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="certainty").text

                cap_area_desc = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="areaDesc").text
                cap_area_desc_processed = replace_problematic_chars_w_underscore(string=cap_area_desc)

                polygon_elem = handle_tag_name_excess(
                    xml_extraction_func=extract_first_immediate_child_feature_from_element,
                    element=data,
                    tag_name="polygon")
                cap_polygon = process_polygon_elem_result(poly_elem=polygon_elem)

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

    # Need to build the values string statements for use later on with sql insert statement.
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

    # exit()

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
        #   insert quantity in excess of 1000 record sql limit.
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
