"""
REDESIGN

"""
# TODO: Stopped so very early on in the process. cgis_MapToSQL can't be imported because it imports configs too and blah


def main():

	# IMPORTS
	from datetime import datetime
	from bs4 import BeautifulSoup
	# from Original_Scripts.cgis_Configs import getConfig
	# from Original_Scripts.cgis_Configs import getMappingInfo
	# from Original_Scripts.cgis_MapToSQL import applyDataMap
	# from Original_Scripts.cgis_Parsers import parseHTMLTable
	# from Original_Scripts.cgis_Parsers import parseSingleHTMLElement
	# from Original_Scripts.cgis_Requests import simpleGetRequest
	# from Original_Scripts.cgis_Transforms import transformHospitalData
	# from Original_Scripts.cgis_Updates import runSQL
	# from Original_Scripts.cgis_Updates import updateTaskTracking
	from time import strftime
	import configparser
	import os
	import pandas as pd
	import requests
	import sys
	import traceback
	import textwrap

	# VARIABLES
	_root_file_path = os.path.dirname(__file__)
	config_file_path = r"doit_config_HospitalStatus.cfg"
	config_section_name = "HospitalStatus"
	config_section_value_of_interest = "url"
	html_id_event_datetime = "lblTime"
	hospitalStatusInfo = {
		"details":
			{"tablename": "RealTime_HospitalStatus"},
		"mapping": [
			{"input": "Hospital", "output": "Linkname", "type": "string"},
			{"input": "Status", "output": "Status", "type": "string"},
			{"input": "Red_Alert", "output": "Red", "type": "string"},
			{"input": "Yellow_Alert", "output": "Yellow", "type": "string"},
			{"input": "Mini_Disaster", "output": "Mini", "type": "string"},
			{"input": "ReRoute", "output": "ReRoute", "type": "string"},
			{"input": "Trauma_ByPass", "output": "t_bypass", "type": "string"},
			{"input": "DataGenerated", "output": "DataGenerated", "type": "datetime %A, %B %d, %Y %I:%M:%S %p"},
		]
	}
	html_id_hospital_table = "tblHospitals"
	realtime_hospitalstatus_headers = ("Linkname", "Status", "Yellow", "Red", "Mini", "ReRoute", "t_bypass",
									   "DataGenerated")
	realtime_hospstat_tbl = "RealTime_HospitalStatus"
	sql_delete_insert_template = textwrap.dedent(
		"""DELETE FROM {realtime_hospstat_tbl}; INSERT INTO {realtime_hospstat_tbl} ({headers_joined})""")
	sql_statements_list = []
	sql_values_statement = """VALUES ({values})"""
	# ASSERT STATEMENTS
	assert os.path.exists(config_file_path)

	# FUNCTIONS
	def create_date_time_value():
		return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

	def grab_single_html_element(html, element_id):
		"""
		pulled from original design except that original returned contents instead of element
		:param html:
		:param element_id:
		:return:
		"""
		soup = BeautifulSoup(markup=html, features="lxml")
		element = soup.find(id=element_id)
		return element

	def setup_config(cfg_file):
		cfg_parser = configparser.ConfigParser()
		cfg_parser.read(filenames=cfg_file)
		return cfg_parser

	# def grab_config_value(cfg_parser, key):
	# 	pass

	def write_response_to_html(response_content, filename):
		with open(filename, "w") as handler:
			handler.write(response_content)
		return

	# FUNCTIONALITY

	# get current datetime stamp
	current_date_time = str(create_date_time_value())

	# need parser to access credentials
	parser = setup_config(config_file_path)

	# read parser file urls_csv_list, split on commas to get list of three urls
	urls_csv_list = parser[config_section_name][config_section_value_of_interest].split(",")

	headers_joined = ",".join([f"'{val}'" for val in realtime_hospitalstatus_headers])
	sql_delete_insert_string = sql_delete_insert_template.format(realtime_hospstat_tbl=realtime_hospstat_tbl,
													headers_joined=headers_joined)
	sql_statements_list.append(sql_delete_insert_string)

	# for each url in the list need to get data, parse data, process data, update database
	for url_index, url_string in enumerate(urls_csv_list):
		output_filename_path = f"{_root_file_path}/data/HospitalStatus_{url_index}.html"

		# Make request to url
		try:
			response = requests.get(url=url_string, params={})
		except Exception as e:
			print(f"Exception during request for html page {url_string}. {e}")
			exit()

		# Old process wrote html page contents to file. Do not know how/if files are used. Preserving process.
		try:
			write_response_to_html(response_content=response.text, filename=output_filename_path)
		except Exception as e:
			print(f"Exception during writing of html file {output_filename_path}. {e}")
			exit()

		# Need to grab the date and time value
		try:
			created_date_element = grab_single_html_element(html=response.content, element_id=html_id_event_datetime)
		except Exception as e:
			print(f"Exception during extraction of datetime from html page {url_string}. {e}")
			exit()
		else:
			created_date_list = list(created_date_element.contents)
			created_date_string = str(created_date_list[0])

		# Old process comment said 'Transform' and nothing else.
		# Now using pandas to get html table.
		html_table_dfs_list = pd.read_html(io=response.text, header=0, attrs={"id": html_id_hospital_table})
		html_table_df = html_table_dfs_list[0]  # html id's are unique so should only be one item in list

		# exit()
		# TODO: build out the needs using new design
		# This iteration will provide a row index value and the row data as a dictionary.
		row_generator = html_table_df.iterrows()
		for row_index, row_series in row_generator:
			values_from_series = row_series.to_dict().values()
			values_string = [f"'{str(val)}'" for val in values_from_series]
			values_string.append(f"'{created_date_string}'")
			values = ",".join(values_string)
			sql_statements_list.append(sql_values_statement.format(values=values))

			# Basically, building sql statement and all the values for insertion into the database.
			# database tasks. I think, given the pandas dataframe use, that all of this can occur outside the for loop
			# get credentials and establish a database connection
			# sql statement build but depends on if an Overwrite flag is True or False
			# True -> 'delete from {table_name}; INSERT INTO {table_name} ('header1', 'header2', ...) VALUES ('value1',
			# 	'value2', 'value3', .... 'datetime value', etc...)'.format(table_name=)
			# False -> 'INSERT INTO {table_name} ('header1', 'header2', ...) VALUES ('value1', 'value2'
			# 	, 'value3', .... 'datetime value', etc...)'.format(table_name=)

	full_sql_string = " ".join(sql_statements_list)
	exit()

	# The table name for the data is 'RealTime_HospitalStatus'
	# sql = applyDataMap(data, info["mapping"], info["details"]["tablename"], True)
	# basically, create a connection, a cursor, execute the sql statements, commit, delete cursor, close connection
	# runSQL(sql)

	# basically, create database connection,cursor, etc. and update some lastRun value in RealTime_TaskTracking
	# updateTaskTracking('HospitalStatus', 'RealTime_HospitalStatus')
	# TODO: Look at things from the database side. Check all the task tracking tables for activity, and the hospital status table

	# except Exception as e:
	# 	print(e)
	# 	exit()
	# 	#update task tracking to indicate the task ran
	# 	updateTaskTracking('HospitalStatus', 'RealTime_HospitalStatus')
	#
	# 	#print traceback to logfile
	# 	print("Trigger Exception, traceback info forward to log file.")
	# 	with open("logs\\errlog_HospitalStatus.txt","w") as erfile:
	# 		erfile.write("Error in doit_HospitalStatus.py execution: " + strftime("%Y-%m-%d %H:%M:%S") + "\n")
	# 		traceback.print_exc(file=erfile)
	# 		sys.exit(1)
	

if __name__ == "__main__":
	main()


