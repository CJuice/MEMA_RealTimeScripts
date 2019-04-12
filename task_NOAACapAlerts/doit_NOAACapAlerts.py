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
    appended_unnecessary_url = r"{http://www.w3.org/2005/Atom}"
    appended_unnecessary_strings = r""
    config_file = r"doit_config_NOAACapAlerts.cfg"
    config_file_path = os.path.join(_root_file_path, config_file)
    database_connection_string = "DSN={database_name};UID={database_user};PWD={database_password}"
    # mdc_code_template = "MDC{fips_last_three}"
    # noaa_fips_values = [24001, 24003, 24005, 24510, 24009, 24011, 24013, 24015, 24017, 24019, 24021, 24023, 24025,
    #                     24027, 24029, 24031, 24033, 24035, 24037, 24039, 24041, 24043, 24045, 24047]
    mdc_code_template = "ILC{fips_last_three}"  # TESTING
    noaa_fips_values = [17003, 17053, 17063, 17075]  # TESTING
    noaa_url_template = r"""http://alerts.weather.gov/cap/wwaatmget.php?x={code}&y=0"""

    # ASSERTS
    assert os.path.exists(config_file_path)
    print(f"Assertion tests completed.")

    # CLASSES
    @dataclass
    class CAPEntry:
        cap_area_desc: str = np.NaN
        cap_certainty: str = np.NaN
        cap_effective: str = np.NaN
        sum_event: str = np.NaN
        cap_expires: str = np.NaN
        cap_mary: str = np.NaN
        cap_msg_type: str = np.NaN
        cap_polygon: str = np.NaN
        cap_severity: str = np.NaN
        cap_status: str = np.NaN
        cap_urgency: str = np.NaN
        data_gen: datetime = np.NaN
        fips: str = np.NaN
        link: str = np.NaN
        published: str = np.NaN
        title: str = np.NaN
        updated: str = np.NaN

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
            if len(result) == 0:
                # NOTE: The 'r' in front of the url is essential for this to work.
                altered_tag_name = appended_unnecessary_url + tag_name
                # print(f"Altering...{altered_tag_name}")
                return element.findall(altered_tag_name)
            else:
                return result

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
            if result is None:
                # NOTE: The 'r' in front of the url is essential for this to work.
                altered_tag_name = appended_unnecessary_url + tag_name
                # print(f"Altering...{altered_tag_name}")
                return element.find(altered_tag_name)
            else:
                return result

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
                return xml_extraction_func(element=element, tag_name=tag_name)
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
        return date_parser.parse(date_string).strftime('%Y-%m-%d %H:%M:%S')

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
    # FIXME: CHANGE OUT FOR SERVER ENVIRONMENT
    # database_cfg_section_name = determine_database_config_value_based_on_script_name()
    print("Using hardcoded database environment variable !!!!!!!!!!!!!!!!!!!!!!!!")
    database_cfg_section_name = "DATABASE_DEV"

    # need a current datetime stamp for database entry
    start_date_time = create_date_time_value_for_db()

    # need parser to access credentials
    config_parser = setup_config(config_file_path)

    noaa_cap_alerts_urls_dict = assemble_fips_to_mdccode_dict(url_template=noaa_url_template,
                                                              mdc_code_template=mdc_code_template,
                                                              fips_values=noaa_fips_values)

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
        alert_objects = []
        for data in entry_element:
            title_text = handle_tag_name_excess(xml_extraction_func=extract_first_immediate_child_feature_from_element,
                                                element=data,
                                                tag_name="title").text
            print(fips, title_text)
            continue
            if title_text == "There are no active watches, warnings or advisories":
                alert_objects.append(CAPEntry(data_gen=date_updated, fips=fips, title=title_text))
                print(title_text)
                break
            else:
                for feature in data:
                    print(feature.tag, feature.attrib, feature.text)
                continue
                link = extract_first_immediate_child_feature_from_element(element=data,
                                                                          tag_name="link").attrib.get("href", np.NaN)
                published = extract_first_immediate_child_feature_from_element(element=data, tag_name="published").text
                published_processed = str(date_parser.parse(published))
                updated = extract_first_immediate_child_feature_from_element(element=data, tag_name="updated").text
                summary = extract_first_immediate_child_feature_from_element(element=data, tag_name="summary").text
                cap_event = extract_first_immediate_child_feature_from_element(element=data, tag_name="cap:event").text
                # cap_effective = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                    tag_name="cap:effective").text
                # cap_expires = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                  tag_name="cap_expires").text
                # cap_status = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                 tag_name="cap_status").text
                # cap_msg_type = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                   tag_name="cap:msgType").text
                # cap_urgency = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                 tag_name="cap:urgency").text
                # cap_severity = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                 tag_name="cap:severity").text
                # cap_certainty = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                 tag_name="cap:certainty").text
                # cap_area_desc = extract_first_immediate_child_feature_from_element(element=data,
                #                                                                 tag_name="cap:areaDesc").text
                print(link)
                print(published_processed)
                print(updated)
                print(summary)
                print(cap_event)
                # print(cap_effective)
                # print(cap_expires)
                # print(cap_status)
                # print(cap_msg_type)
                # print(cap_urgency)
                # print(cap_severity)
                # print(cap_certainty)
                # print(cap_area_desc)
                print()

if __name__ == "__main__":
    main()
