#!/usr/bin/python
#
# The purpose of this python script is to collect metrics for hash join node.
# Given the impala query profile file as the input, 
# this script iterate through the file;
# skip 'averaged fragment' part; 
# collect all hash join related metrics from the 'instance fragment' part;
#
# The output of the script is a comma seperated csv file;
#
import sys
import os
import string
import re
import csv


from scripts.utility import *
def collect_statistics(profile_path, output_file_path, output_dir_path="."):
	global summary_geomean_deserialized
	global summary_geomean_converter
	global geomean_map

	keys = ['node id',                     # index 0
			'row size (bytes)',            # index 1 
			'BytesReceived',               # index 2
         	'ConvertRowBatchTime',         # index 3
         	'DeserializeRowBatchTimer',    # index 4
         	'FirstBatchArrivalWaitTime',   # index 5
         	'MergeGetNext',                # index 6
         	'MergeGetNextBatch',           # index 7
         	'PeakMemoryUsage',             # index 8
         	'RowsReturned',                # index 9
         	'RowsReturnedRate',            # index 10
         	'SendersBlockedTimer',         # index 11
         	'SendersBlockedTotalTimer(*)', # index 12
         	'Converter Cost (bytes per ns)',   # index 13
         	'Deserialized Cost (bytes per ns)' # index 14         	
		    ]

	func_map = {}
	func_map['BytesReceived'] = get_count
	func_map['ConvertRowBatchTime'] = get_time
	func_map['DeserializeRowBatchTimer'] = get_time
	func_map['FirstBatchArrivalWaitTime'] = get_time
	func_map['MergeGetNext'] = get_count
	func_map['MergeGetNextBatch'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['RowsReturned'] = get_count
	func_map['RowsReturnedRate'] = get_row_return_rate
	func_map['SendersBlockedTimer'] = get_time
	func_map['SendersBlockedTotalTimer(*)'] = get_time
	
	record = [0] * len(keys)
	row_size_map = {}

	average_fragment_metrics_skipped = False
	within_exchange_entry = False
	within_plan_fragment_entries = False
	within_exchange_plan_node = False
	within_coordinator_fragment = False
	
	node_id = 0
	geomean_deserialize_cost_bytes_per_ns = 1.0	
	geomean_convert_cost_bytes_per_ns = 1.0	

	sample_deserialize_cost_bytes_per_ns = []
	sample_convert_cost_bytes_per_ns = []

	derialize_cost_bytes_per_ns_per_node_map = {}
	convert_cost_bytes_per_ns_per_node_map = {}

	sample_set = [0.0]*4

	for line in open(profile_path):
		stripped_line = line.strip();

		if stripped_line.startswith("Estimated Per-Host Requirements:"):
			within_plan_fragment_entries = True
		elif stripped_line.startswith("Estimated Per-Host Mem:"):
			within_plan_fragment_entries = False				
		elif (stripped_line.find(":EXCHANGE") != -1 or stripped_line.find(":MERGING-EXCHANGE") != -1) and within_plan_fragment_entries:
			# get node id
			p = re.compile('\d+(:.*EXCHANGE)')
			m = p.search(stripped_line)
			start_index = m.span()[0]
			end_index = m.span()[1]
			stripped_line = stripped_line[start_index:end_index]				
			while stripped_line.startswith('0'):
				stripped_line = stripped_line[1:]
			lft_idx = 0
			rgh_idx = stripped_line.find(":")
			node_id = stripped_line[lft_idx:rgh_idx]	
			within_exchange_plan_node = True
		elif within_exchange_plan_node and stripped_line.find("row-size") != -1:
			lft_idx = stripped_line.find("row-size=")
			rgh_idx = stripped_line.find("B cardinality")
			row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]
			node_id = 0
			within_exchange_plan_node = False
		elif stripped_line.startswith("Instance") and not average_fragment_metrics_skipped:
			average_fragment_metrics_skipped = True
		elif stripped_line.startswith("Coordinator Fragment"):
			within_coordinator_fragment = True					
		elif stripped_line.startswith("Averaged Fragment"):
			within_coordinator_fragment = False
		elif stripped_line.startswith("EXCHANGE_NODE (id=") and (within_coordinator_fragment or average_fragment_metrics_skipped):
			curr_id = get_exec_node_id(stripped_line)
			record[0] = curr_id
			record[1] = row_size_map[curr_id]
			within_exchange_entry = True
		elif (within_coordinator_fragment or average_fragment_metrics_skipped) and within_exchange_entry:

			key = get_label(stripped_line)
			
			if key != '' and key in func_map:
				value = func_map[key](stripped_line)
				record[keys.index(key)] = value
				#print('key: {0}; value: {1}'.format(key, value))	
				if key == 'SendersBlockedTotalTimer(*)':
					
					if float(record[2]) != 0 and float(record[3]) != 0:
						convert_cost_bytes_per_ns = float(record[2]) / float(record[3])
						record[len(keys)-2] = convert_cost_bytes_per_ns
						sample_convert_cost_bytes_per_ns.append(convert_cost_bytes_per_ns)
						sample_set[0] += float(record[2])
						sample_set[1] += float(record[3])
					else:
						convert_cost_bytes_per_ns = 0

					if curr_id not in convert_cost_bytes_per_ns_per_node_map.keys():
						convert_cost_bytes_per_ns_per_node_map[curr_id] = []
					convert_cost_bytes_per_ns_per_node_map[curr_id].append(convert_cost_bytes_per_ns)

					if float(record[2]) != 0 and float(record[4]) != 0:

						deserialize_cost_bytes_per_ns = float(record[2]) / float(record[4])
						record[len(keys)-1] = deserialize_cost_bytes_per_ns
						sample_deserialize_cost_bytes_per_ns.append(deserialize_cost_bytes_per_ns)
						sample_set[2] += float(record[2])
						sample_set[3] += float(record[4])
					else:
						deserialize_cost_bytes_per_ns = 0
					
					if curr_id not in derialize_cost_bytes_per_ns_per_node_map.keys():
						derialize_cost_bytes_per_ns_per_node_map[curr_id] = []
					derialize_cost_bytes_per_ns_per_node_map[curr_id].append(deserialize_cost_bytes_per_ns)

					within_exchange_entry = False
					
					record = [0] * len(keys)
					
		# enf if
	# end for		

	record = [''] * 13

	with open(output_file_path, 'a', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')
		for key in convert_cost_bytes_per_ns_per_node_map.keys():
			csv_writer.writerow(['EXCH_ROW_BATCH_CONVERT_COST', key, '{0:.8f}'.format(average(convert_cost_bytes_per_ns_per_node_map[key]))])	
		for key in derialize_cost_bytes_per_ns_per_node_map.keys():
			csv_writer.writerow(['EXCH_DESER_COST', key, '{0:.8f}'.format(average(derialize_cost_bytes_per_ns_per_node_map[key]))])

if __name__ == '__main__':

	import platform
	print(platform.python_version())

	# un-comment the following two lins if you want to run the script through an IDE/Python Editor
	# profile = os.path.join('.', 'q98.sql_1427955243', 'profiles', 'q98.sql.log')
	profile = r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_2.7G_PARQUET_50\q52.sql.log'
	output = r'C:\Development\logs\metrics\exchange'
	sys.argv = ['exchange.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: exchange.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_statistics, os.path.normpath(profile), os.path.normpath(output))
