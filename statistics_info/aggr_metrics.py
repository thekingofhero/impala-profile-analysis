import sys
import os
import csv
from scripts.utility import *
from statistics import *

def collect_statistics(profile_path, output_file_path,file_p , output_dir_path="."):
	
	keys = ['Node Id',                  # index 0
			'Row Size (Bytes)',         # index 1			
			'Row Size (Build)',         # index 2
			'BuildTime',                # index 3
			'GetNewBlockTime',          # index 4
			'GetResultsTime',           # index 5
			'HashBuckets',              # index 6
			'LargestPartitionPercent',  # index 7
			'MaxPartitionLevel',        # index 8
			'NumRepartitions',          # index 9
			'PartitionsCreated',        # index 10
			'PeakMemoryUsage',          # index 11
			'PinTime',                  # index 12
			'RowsRepartitioned',        # index 13
			'RowsReturned',             # index 14
			'RowsReturnedRate',         # index 15
			'SpilledPartitions',        # index 16			
			'UnpinTime',                # index 17
			'Row Count (Build)',        # index 18
			'Rows Per NS',              # index 19
			'Bytes Per NS'              # index 20
	]

	func_map = {}
	func_map['BuildTime'] = get_time
	func_map['GetNewBlockTime'] = get_time
	func_map['GetResultsTime'] = get_time
	func_map['HashBuckets'] = get_count
	func_map['LargestPartitionPercent'] = get_count
	func_map['MaxPartitionLevel'] = get_count
	func_map['NumRepartitions'] = get_count
	func_map['PartitionsCreated'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['PinTime'] = get_time
	func_map['RowsRepartitioned'] = get_count
	func_map['RowsReturned'] = get_count
	func_map['RowsReturnedRate'] = get_row_return_rate
	func_map['SpilledPartitions'] = get_count
	func_map['UnpinTime'] = get_time

	record = []
	prev_record = []
	row_size_map = {}
	build_row_size_map = {}
	
	within_plan_fragment_entries = False
	average_fragment_metrics_skipped = False
	within_aggregate_plan_node = False
	within_aggregate_entry = False
	last_entry_processed = False
	within_coordinator_fragment = False
	has_get_new_block_time = False

	is_merge_aggr = False

	node_id = 0
	prev_id = 0
	
	geomean_rows_per_ns = 1.0

	find_build_row_size = False
	find_rows_returned = False

	rows_per_ns_per_aggregate_node = {}
	
	with open(output_file_path, 'w', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		header = []
		for key in keys:
			header.append(key)
		csv_writer.writerows([header])

		for line in open(profile_path):
			stripped_line = line.strip()
			
			if stripped_line.startswith("Estimated Per-Host Requirements:"):
				within_plan_fragment_entries = True
			elif stripped_line.startswith("Estimated Per-Host Mem:"):
				within_plan_fragment_entries = False
			elif stripped_line.startswith("Instance") and not average_fragment_metrics_skipped:
				average_fragment_metrics_skipped = True
			elif stripped_line.startswith("Coordinator Fragment"):
				within_coordinator_fragment = True					
			elif stripped_line.startswith("Averaged Fragment"):
				within_coordinator_fragment = False
			elif stripped_line.find(":AGGREGATE") != -1 and within_plan_fragment_entries:				
				node_id = -1
				rgh_idx = stripped_line.find(":AGGREGATE")				
				lft_idx = 0
				if stripped_line.startswith('0'):
					lft_idx = 1
				node_id = stripped_line[lft_idx:rgh_idx]
				
				within_aggregate_plan_node = True
			elif within_aggregate_plan_node and stripped_line.find("row-size") != -1:
				lft_idx = stripped_line.find("row-size=")
				rgh_idx = stripped_line.find("B cardinality")
				row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]

				if find_build_row_size:
					build_row_size_map[prev_id] = row_size_map[node_id]
					prev_id = -1

				prev_id = node_id
				within_aggregate_plan_node = False
				find_build_row_size = True
			elif find_build_row_size and stripped_line.find("row-size") != -1:
				find_build_row_size = False
				lft_idx = stripped_line.find("row-size=")
				rgh_idx = stripped_line.find("B cardinality")				
				build_row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]	
			elif find_rows_returned and stripped_line.find("RowsReturned:") != -1:
				find_rows_returned = False

				# for this case, we have already encountered an aggregation node
				if len(prev_record) > 0:		
					curr_id = prev_record[0]
					tmp = record
					record = prev_record
					prev_record = tmp				
					find_rows_returned = True
				
				record.append(get_count(stripped_line))

				rows_per_ns = float(record[18]) / float(record[3])
				record.append(rows_per_ns)

				if curr_id not in rows_per_ns_per_aggregate_node.keys():
					rows_per_ns_per_aggregate_node[curr_id] = []
				rows_per_ns_per_aggregate_node[curr_id].append(rows_per_ns)

				csv_writer.writerows([record])

				if find_rows_returned:
					record = prev_record
					prev_record = []
					curr_id = record[0]
					find_rows_returned = False					
					record.append(get_count(stripped_line))	
					
				else:
					record = []

			elif stripped_line.startswith("AGGREGATION_NODE (id=") and (within_coordinator_fragment or average_fragment_metrics_skipped):	
				# +1 aggregation node
				if find_rows_returned:
					prev_record = record
					record = []
				
				curr_id = get_exec_node_id(stripped_line)
				
				record.append(curr_id)
				record.append(row_size_map[curr_id])
				record.append(build_row_size_map[curr_id])
				within_aggregate_entry = True				
			elif (within_coordinator_fragment or average_fragment_metrics_skipped) and within_aggregate_entry:
				
				key = get_label(stripped_line)
				if key != '' and key in func_map:
					record.append(func_map[key](stripped_line))
					
					if (key == 'PeakMemoryUsage' or key == 'SpilledPartitions' or key == 'GetResultsTime') and not has_get_new_block_time:
						record.append(0)

					if key == 'UnpinTime' or (key == 'SpilledPartitions' and not has_get_new_block_time):
						within_aggregate_entry = False
						find_rows_returned = True	
						has_get_new_block_time = False
					elif key == 'GetNewBlockTime':
						has_get_new_block_time = True
						
		# end for

		csv_writer.writerows([record])
	
	for key in rows_per_ns_per_aggregate_node.keys():
		print('AGGREGATE_BUILD_COST,{0},{1:.6f}'.format(key, average(rows_per_ns_per_aggregate_node[key])),file = file_p)	
	
if __name__ == '__main__':

	import platform
	print(platform.python_version())

	query_name = 'ss_max.sql.log'
	profile = os.path.join(r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_ALL_TEXT_50\2.7gz', query_name)
	output = r'C:\Development\logs\metrics\aggregation'
	sys.argv = ['aggr_metrics.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: aggr_metrics.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_statistics, os.path.normpath(profile), os.path.normpath(output))
	