"""
REDESIGN

"""


def main():

	# IMPORTS
	from datetime import datetime
	# from Original_Scripts.cgis_Configs import getConfig
	# from Original_Scripts.cgis_Configs import getMappingInfo
	# from Original_Scripts.cgis_MapToSQL import applyDataMap
	# from Original_Scripts.cgis_Parsers import parseHTMLTable
	# from Original_Scripts.cgis_Parsers import parseSingleHTMLElement
	# from Original_Scripts.cgis_Requests import simpleGetRequest
	# from Original_Scripts.cgis_Transforms import transformHospitalData
	# from Original_Scripts.cgis_Updates import runSQL
	# from Original_Scripts.cgis_Updates import updateTaskTracking
	# from time import strftime
	import configparser
	import numpy as np
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
	sql_delete_insert_template = """DELETE FROM {realtime_hospstat_tbl}; INSERT INTO {realtime_hospstat_tbl} ({headers_joined})"""
	sql_statements_list = []
	sql_values_statement = """VALUES ({values})"""
	sql_values_string_template = """'{hospital}', '{status_level_value}', '{red_alert}','{yellow_alert}', '{mini_disaster}', '{reroute}', '{trauma_bypass}', '{created_date_string}'"""

	# ASSERT STATEMENTS
	assert os.path.exists(config_file_path)

	# FUNCTIONS
	def create_date_time_value():
		return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

	# def determine_capacity_value(html_row_series: pd.Series):
	# 	"""
	# 	Was not used in final output in original script but value was detected/handled.
	# 	:param html_row_series:
	# 	:return:
	# 	"""
	# 	return html_row_series.get(key="Capacity", default=np.NaN)

	def determine_status_level(html_row_series: pd.Series):
		"""Evaluate presence of data in html table and return string based on business logic tree.
		This is reproduced functionality from interpretation of single line statement in old code
		that determined 'Status' dictionary keys corresponding value
		OLD: "red" if row[4] is not '' else "yellow" if row[3] is not '' or row[6] is not ''
				else "t_bypass" if row[7] is not '' else "mini" if row[5] is not '' else "normal"
		'row' was a record from an html table, with two values appended at the beginning. The old process was
		basically looking for a value other than null/empty and there is a hierarchy of importance if values
		are simultaneously present. The old way created a list ('row') that started with created date and current date,
		then the row contents from html table. Redesign subtracts two from old index positions since the two date
		values are no longer a factor.
		"""
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

	# def format_data_created_value(date_string: str) -> str:
	# 	return dateparser.parse(date_string)
	#
	# def grab_single_html_element(html, element_id):
	# 	"""
	# 	pulled from original design except that original returned contents instead of element
	# 	:param html:
	# 	:param element_id:
	# 	:return:
	# 	"""
	# 	soup = BeautifulSoup(markup=html, features="lxml")
	# 	element = soup.find(id=element_id)
	# 	return element

	def setup_config(cfg_file):
		cfg_parser = configparser.ConfigParser()
		cfg_parser.read(filenames=cfg_file)
		return cfg_parser

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

		# Now using pandas to get html table.
		html_table_dfs_list = pd.read_html(io=response.text, header=0, attrs={"id": html_id_hospital_table})
		html_table_df = html_table_dfs_list[0]  # html id's are unique so should only be one item in list

		# This iteration will provide a row index value and the row data as a dictionary.
		row_generator = html_table_df.iterrows()
		for row_index, row_series in row_generator:
			# capacity_value = determine_capacity_value(row_series)	# Capacity was detected/handled but not output to db
			status_level_value = determine_status_level(row_series)
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
	full_sql_string = " ".join(sql_statements_list)
	exit()

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


