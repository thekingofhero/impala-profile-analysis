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

	keys = ['Node Id',                   # index 0
			'Row Size (Build)',          # index 1
			'Row Size (Probe)',	         # index 2
			'Build Partition Time',      # index 3
			'Build Rows',                # index 4
			'Build Rows Partitioned',    # index 5
			'Build Time',                # index 6
			'Get New Block Time',        # index 7
			'Hash Buckets',              # index 8
			'Largest Partition Percent', # index 9
			'Max Partition Level',       # index 10
			'Num Repartitions',          # index 11
			'Partitions Created',        # index 12
			'Peak Memory Usage',         # index 13
			'Pin Time',                  # index 14
			'Probe Rows',                # index 15
			'Probe Rows Partitioned',    # index 16
			'Probe Time',                # index 17
			'Rows Returned',             # index 18
			'Rows Returned Rate',        # index 19
			'Spilled Partitions',        # index 20
			'Unpin Time',                # index 21
			'Bytes Per NS (Build)',      # index 22
			'Bytes Per NS (Probe)',      # index 23
			]

	func_map = {}
	func_map['BuildPartitionTime'] = get_time
	func_map['BuildRows'] = get_count
	func_map['BuildRowsPartitioned'] = get_count
	func_map['BuildTime'] = get_time
	func_map['GetNewBlockTime'] = get_time
	func_map['HashBuckets'] = get_count
	func_map['LargestPartitionPercent'] = get_count
	func_map['MaxPartitionLevel'] = get_count
	func_map['NumRepartitions'] = get_count
	func_map['PartitionsCreated'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['PinTime'] = get_time
	func_map['ProbeRows'] = get_count
	func_map['ProbeRowsPartitioned'] = get_count
	func_map['ProbeTime'] = get_time
	func_map['RowsReturned'] = get_count
	func_map['RowsReturnedRate'] = get_row_return_rate
	func_map['SpilledPartitions'] = get_count
	func_map['UnpinTime'] = get_time

	probe_row_size_map = {}
	build_row_size_map = {}

	average_fragment_metrics_skipped = False
	within_hash_join_entry = False
	within_plan_fragment_entries = False
	within_hash_join_plan_node = False

	record = []

	node_id = -1
	prev_id = -1
	find_build_row_size = False
	find_probe_row_size = False

	geomean_build_bytes_per_ns = 1.0
	geomean_probe_bytes_per_ns = 1.0

	sample_build_bytes_per_ns = []
	sample_probe_bytes_per_ns = []

	probe_rows_per_ns_hashjoin_node = {}
	build_rows_per_ns_hashjoin_node = {}

	non_child_time = 0.0

	for line in open(profile_path):
		stripped_line = line.strip();

		if stripped_line.startswith("Estimated Per-Host Requirements:"):
			within_plan_fragment_entries = True
		elif stripped_line.startswith("Estimated Per-Host Mem:"):
			within_plan_fragment_entries = False
		elif stripped_line.find(":HASH JOIN") != -1 and within_plan_fragment_entries:
			# get hash join node id
			rgh_idx = stripped_line.find(":HASH JOIN")
			lft_idx = 0
			if stripped_line.startswith('0'):
				lft_idx = 1
			node_id = stripped_line[lft_idx:rgh_idx]
			within_hash_join_plan_node = True
		elif within_hash_join_plan_node and stripped_line.find("row-size") != -1:
			if find_build_row_size:
				find_build_row_size = False
				find_probe_row_size = True

				lft_idx = stripped_line.find("row-size=")
				rgh_idx = stripped_line.find("B cardinality")
				build_row_size_map[prev_id] = stripped_line[lft_idx+9:rgh_idx]
			if find_probe_row_size:
				find_probe_row_size = False

				lft_idx = stripped_line.find("row-size=")
				rgh_idx = stripped_line.find("B cardinality")
				probe_row_size_map[prev_id] = stripped_line[lft_idx+9:rgh_idx]
				prev_id = -1

			prev_id = node_id
			within_hash_join_plan_node = False
			find_build_row_size = True
		elif find_build_row_size and stripped_line.find("row-size") != -1:
			find_build_row_size = False
			find_probe_row_size = True

			lft_idx = stripped_line.find("row-size=")
			rgh_idx = stripped_line.find("B cardinality")
			build_row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]
		elif find_probe_row_size and stripped_line.find("row-size") != -1:
			find_probe_row_size = False
			lft_idx = stripped_line.find("row-size=")
			rgh_idx = stripped_line.find("B cardinality")
			probe_row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]
			node_id = -1
		elif stripped_line.startswith("Instance") and not average_fragment_metrics_skipped:
			average_fragment_metrics_skipped = True
		elif stripped_line.startswith("HASH_JOIN_NODE (id=") and average_fragment_metrics_skipped:

			non_child_time = float(get_non_child_time_in_str(stripped_line))

			curr_id = get_exec_node_id(stripped_line)
			record.append(curr_id)
			record.append(build_row_size_map[curr_id])
			record.append(probe_row_size_map[curr_id])
			within_hash_join_entry = True
		elif average_fragment_metrics_skipped and within_hash_join_entry:

			key = get_label(stripped_line)
			if key != '' and key in func_map:
				record.append(func_map[key](stripped_line))

				if key == 'UnpinTime':

					build_phase_rows_per_ns = 0.0
					probe_phase_rows_per_ns = 0.0

					build_phase_time = float(record[6])
					probe_phase_time = float(record[17])
					build_rows = float(record[4])
					probe_rows = float(record[15])
					# print(curr_id, non_child_time, build_phase_time, probe_phase_time)
					if build_phase_time != 0 and build_rows != 0:
						build_phase_rows_per_ns = build_rows / build_phase_time
					else:
						build_phase_rows_per_ns = 0
						

					if probe_rows != 0 and probe_phase_time != 0:
						probe_phase_rows_per_ns = probe_rows / probe_phase_time
					else:
						probe_phase_rows_per_ns = 0

					# if non_child_time > 1000000000:
					# 	# calculate the ratio 
					#hash_join_time = build_phase_time + probe_phase_time
					#build_phase_ratio = build_phase_time / hash_join_time
					#probe_phase_ratio = probe_phase_time / hash_join_time

					#build_phase_rows_per_ns	=  float(record[4]) / (non_child_time * build_phase_ratio)
					#probe_phase_rows_per_ns	= float(record[15]) / (non_child_time * probe_phase_ratio)

					record.append("{0:.8f}".format(build_phase_rows_per_ns))
					if curr_id not in build_rows_per_ns_hashjoin_node.keys():
						build_rows_per_ns_hashjoin_node[curr_id] = []
					build_rows_per_ns_hashjoin_node[curr_id].append(build_phase_rows_per_ns)

					record.append("{0:.8f}".format(probe_phase_rows_per_ns))
					if curr_id not in probe_rows_per_ns_hashjoin_node.keys():
						probe_rows_per_ns_hashjoin_node[curr_id] = []
					probe_rows_per_ns_hashjoin_node[curr_id].append(probe_phase_rows_per_ns)

					within_hash_join_entry = False
					record = []

		# enf if
	# end for

	# end with
	with open(output_file_path, 'a', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		for key in build_rows_per_ns_hashjoin_node.keys():
			csv_writer.writerow(['HASHJOIN_BUILD_COST', key, '{0:.8f}'.format(average(build_rows_per_ns_hashjoin_node[key]))])

		for key in probe_rows_per_ns_hashjoin_node.keys():
			csv_writer.writerow(['HASHJOIN_PROBE_COST', key, '{0:.8f}'.format(average(probe_rows_per_ns_hashjoin_node[key]))])

if __name__ == '__main__':

	import platform
	print(platform.python_version())

	query_name = 'q27.sql.log'
	profile = os.path.join(r'C:\Users\heheda2\Syncplicity\Benchmarks\executions\1G_2.7G_PARQUET_3000\parquet', query_name)
	output = r'C:\Development\logs\metrics\hash-join'
	sys.argv = ['hj_metrics.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: hj_metrics.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_statistics, os.path.normpath(profile), os.path.normpath(output))
