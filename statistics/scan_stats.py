import sys
import os
import string
import csv
from scripts.utility import *
from statistics import *

server_idx_map = {'tracing024':0, 
				  'tracing025':1,
				  'tracing026':2,
				  'tracing027':3}

total_num_of_disks = 0.0

def collect_scanner_stats_info(profile_path, output_file_path, output_dir_path="."):
	global total_num_of_disks

	keys = ['Node Id',                   				   # index 0
			'Row Size (bytes)',          				   # index 1
			'AverageHdfsReadThreadConcurrency',            # index 2
			'AverageScannerThreadConcurrency',             # index 3
			'BytesRead',                                   # index 4
			'BytesReadDataNodeCache',                      # index 5
			'BytesReadLocal',                              # index 6
			'BytesReadShortCircuit',                       # index 7
			'DecompressionTime',                           # index 8					
			'MaxCompressedTextFileLength',                 # index 9
			'NumColumns',                                  # index 10
			'NumDisksAccessed',                            # index 11
			'NumScannerThreadsStarted',                    # index 12
			'PeakMemoryUsage',                             # index 13
			'PerReadThreadRawHdfsThroughput',              # index 14
			'RowsRead',                                    # index 15
			'RowsReturned',                                # index 16
			'RowsReturnedRate',                            # index 17
			'ScanRangesComplete',                          # index 18
			'ScannerThreadsInvoluntaryContextSwitches',    # index 19
			'ScannerThreadsTotalWallClockTime',            # index 20
			'DelimiterParseTime',                          # index 21
			'MaterializeTupleTime(*)',                     # index 22
			'ScannerThreadsSysTime',                       # index 23
			'ScannerThreadsUserTime',                      # index 24
			'ScannerThreadsVoluntaryContextSwitches',      # index 25
			'TotalRawHdfsReadTime(*)',                     # index 26
			'TotalReadThroughput',                         # index 27
			'Parser Delimiter Cost (Bytes/NS)',            # index 28       
			'Materialize Tuple Cost (Bytes/NS)'            # index 29
		    ]

	func_map = {}
	func_map['AverageHdfsReadThreadConcurrency'] = get_count
	func_map['AverageScannerThreadConcurrency'] = get_count
	func_map['BuildRowsPartitioned'] = get_count
	func_map['BytesRead'] = get_count
	func_map['BytesReadDataNodeCache'] = get_count
	func_map['BytesReadLocal'] = get_count
	func_map['BytesReadShortCircuit'] = get_count
	func_map['DecompressionTime'] = get_time
	func_map['MaxCompressedTextFileLength'] = get_count
	func_map['NumColumns'] = get_count
	func_map['NumDisksAccessed'] = get_count
	func_map['NumScannerThreadsStarted'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['PerReadThreadRawHdfsThroughput'] = get_throughput_in_mb
	func_map['RowsRead'] = get_count
	func_map['RowsReturned'] = get_count
	func_map['RowsReturnedRate'] = get_row_return_rate
	func_map['ScanRangesComplete'] = get_count
	func_map['ScannerThreadsInvoluntaryContextSwitches'] = get_count
	func_map['ScannerThreadsTotalWallClockTime'] = get_time
	func_map['DelimiterParseTime'] = get_time
	func_map['MaterializeTupleTime(*)'] = get_time
	func_map['ScannerThreadsSysTime'] = get_time
	func_map['ScannerThreadsUserTime'] = get_time
	func_map['ScannerThreadsVoluntaryContextSwitches'] = get_count
	func_map['TotalRawHdfsReadTime(*)'] = get_time
	func_map['TotalReadThroughput'] = get_throughput_in_mb

	record = []
	row_size_map = {}
	scan_time_map = {}

	node_id = 0

	within_hdfs_scan_node = False
	within_plan_fragment_entries = False
	average_fragment_metrics_skipped = False
	within_hdfs_scan_entry = False
	read_from_cache = False

	parser_delimiter_time_per_node_map = {}
	materialise_tuple_time_per_node_map = {}

	file_format = 'TEXT'
	codec = 'NONE'

	server_id = 0

	with open(output_file_path, 'w', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')
		
		header = []
		for key in keys:
			header.append(key)
		csv_writer.writerows([header])

		for line in open(profile_path):
			stripped_line = line.strip();

			if stripped_line.startswith("Estimated Per-Host Requirements:"):
				within_plan_fragment_entries = True
			elif stripped_line.startswith("Estimated Per-Host Mem:"):
				within_plan_fragment_entries = False				
			elif stripped_line.find(":SCAN HDFS") != -1 and within_plan_fragment_entries:
				# get hash join node id
				rgh_idx = stripped_line.find(":SCAN HDFS")
				lft_idx = 0
				if stripped_line.startswith('0'):
					lft_idx = 1
				node_id = stripped_line[lft_idx:rgh_idx]
				within_hdfs_scan_node = True
			elif within_hdfs_scan_node and stripped_line.find("row-size") != -1:
				lft_idx = stripped_line.find("row-size=")
				rgh_idx = stripped_line.find("B cardinality")
				row_size_map[node_id] = stripped_line[lft_idx+9:rgh_idx]
				node_id = 0
				within_hdfs_scan_node = False
			elif stripped_line.startswith("Instance") :
				lft_idx = stripped_line.find("(host=")
				rgh_idx = stripped_line.find(":22000):(")
				host_name = stripped_line[lft_idx+6:rgh_idx]
				server_id = server_idx_map[host_name]

				average_fragment_metrics_skipped = True
			elif stripped_line.startswith("HDFS_SCAN_NODE (id=") and average_fragment_metrics_skipped:
				curr_id = get_exec_node_id(stripped_line)
				total_time = get_total_time(stripped_line)
				record.append(curr_id)
				record.append(row_size_map[curr_id])
				scan_time_map[curr_id] = total_time
				within_hdfs_scan_entry = True
			elif average_fragment_metrics_skipped and within_hdfs_scan_entry:

				key = get_label(stripped_line)
				
				if key != '':
					if key in func_map:
						
						record.append(func_map[key](stripped_line))
						if key == 'ScannerThreadsTotalWallClockTime' and file_format == 'PARQUET':							
							record.append(0)
						if key == 'MaxCompressedTextFileLength' and file_format == 'TEXT':
							record.append(0)


						if key == 'BytesReadDataNodeCache':
							if float(record[len(record)-1]) > 0:
								read_from_cache = True						
						elif key == 'TotalReadThroughput':			
							within_hdfs_scan_entry = False
							
							# calculate parser delimieter time
							parser_delimiter_time = 0
							if float(record[21]) != 0:
								parser_delimiter_time = float(record[15]) / float(record[21]) 
							record.append(parser_delimiter_time)

							if curr_id not in parser_delimiter_time_per_node_map.keys():
								parser_delimiter_time_per_node_map[curr_id] = []
							parser_delimiter_time_per_node_map[curr_id].append(parser_delimiter_time)
							
							# calculate materialize tuple time
							materialize_tuple_time = float(record[15]) / float(record[22])
							record.append(materialize_tuple_time)

							if curr_id not in materialise_tuple_time_per_node_map.keys():
								materialise_tuple_time_per_node_map[curr_id] = []
							materialise_tuple_time_per_node_map[curr_id].append(materialize_tuple_time)
														
							csv_writer.writerows([record])
							record = []

					elif key == 'File Formats':
						# get file format
						lft_idx = stripped_line.find(':')
						rgh_idx = stripped_line.find('/')
						file_format = stripped_line[lft_idx+1:rgh_idx].strip()
							
						stripped_line = stripped_line[rgh_idx:]
						rgh_idx = stripped_line.find(':')
						codec = stripped_line[1:rgh_idx]

		csv_writer.writerows([record])
		
		for key in parser_delimiter_time_per_node_map.keys():
			print('PARSER_DELIMITER_TIME,{0},{1:.6f}'.format(key, average(parser_delimiter_time_per_node_map[key])))	
		for key in materialise_tuple_time_per_node_map.keys():
			print('MATERIALIZE_TUPLE_TIME,{0},{1:.6f}'.format(key, average(materialise_tuple_time_per_node_map[key])))

		return [parser_delimiter_time_per_node_map, materialise_tuple_time_per_node_map]
		
if __name__ == '__main__':

	import platform
	print(platform.python_version())

	global total_num_of_disks
	total_num_of_disks = 9

	query_name = 'q3.sql.log'
	profile = os.path.join(r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_2.7G_TEXT_3000', query_name)
	output = r'C:\Development\logs\metrics\scan-stats'
	sys.argv = ['scan_stats.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: scan_stats.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_scanner_stats_info, os.path.normpath(profile), os.path.normpath(output), 'csv')
	