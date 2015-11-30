import sys
import os
import string
import csv


from scripts.utility import *

server_idx_map = {'tracing024':0, 
				  'tracing025':1,
				  'tracing026':2,
				  'tracing027':3}
file_format = ''
codec = ''

def collect_scanner_stats_info(profile_path, output_file_path, output_dir_path="."):

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

	delimiter_parse_time_per_node_map = {}
	materialise_tuple_time_per_node_map = {}
	disk_throughput_map = {}
	hdfs_decompression_time_factor_map = {}

	server_id = 0

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

			lft_idx = stripped_line.find(":(Total: ")
			rgh_idx = stripped_line.find(",")
			inst_total_time_in_str = stripped_line[lft_idx+8:rgh_idx]
			inst_total_time_in_str = float(get_time_in_str_ns(inst_total_time_in_str))

			if server_id not in hdfs_decompression_time_factor_map.keys():
				hdfs_decompression_time_factor_map[server_id] = {}

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

						num_disk_accessed          = float(record[11])	
						mb_read                    = float(record[4]) / 1024 / 1024
						
						ideal_scan_time  = mb_read / (100 * num_disk_accessed) * 1000000000
						actual_scan_time = total_time
						scan_time_delta  = 0
						# print(curr_id, ideal_scan_time, actual_scan_time)
						if actual_scan_time > ideal_scan_time:
							scan_time_delta = actual_scan_time - ideal_scan_time
						else:
							scan_time_delta = actual_scan_time

						delimiter_parse_time   = float(record[21]) 
						materialize_tuple_time = float(record[22])

						process_time    = delimiter_parse_time + materialize_tuple_time
						scan_time_delta = scan_time_delta if scan_time_delta < process_time else process_time

						delimiter_parse_time_ratio   = 0 if delimiter_parse_time   == 0 else delimiter_parse_time   / process_time
						materialize_tuple_time_ratio = 0 if materialize_tuple_time == 0 else materialize_tuple_time / process_time
						
						if delimiter_parse_time_ratio > 0:
							delimiter_parse_time = float(record[15]) / (scan_time_delta * delimiter_parse_time_ratio)
						materialize_tuple_time = float(record[15]) / (scan_time_delta * materialize_tuple_time_ratio)

						record.append(delimiter_parse_time)
						record.append(materialize_tuple_time)

						if curr_id not in delimiter_parse_time_per_node_map.keys():
							delimiter_parse_time_per_node_map[curr_id] = []
						delimiter_parse_time_per_node_map[curr_id].append(delimiter_parse_time)

						if curr_id not in materialise_tuple_time_per_node_map.keys():
							materialise_tuple_time_per_node_map[curr_id] = []
						materialise_tuple_time_per_node_map[curr_id].append(materialize_tuple_time)
													
						record = []

					elif key == 'RowsRead':
						if curr_id not in hdfs_decompression_time_factor_map[server_id].keys():
							scanner_concurrency = float(record[3])
							scanner_concurrency = 1 if scanner_concurrency == 0 else scanner_concurrency
							hdfs_decompression_time_factor_map[server_id][curr_id] = float(record[8])/float(record[15])
					elif key == 'PerReadThreadRawHdfsThroughput':
						if server_id not in disk_throughput_map.keys():
							disk_throughput_map[server_id] = {}
						
						disk_throughput_map[server_id][curr_id] = float(record[14])
						


				elif key == 'File Formats':
					# get file format
					lft_idx = stripped_line.find(':')
					rgh_idx = stripped_line.find('/')
					file_format = stripped_line[lft_idx+1:rgh_idx].strip()
						
					stripped_line = stripped_line[rgh_idx:]
					rgh_idx = stripped_line.find(':')
					codec = stripped_line[1:rgh_idx]

	with open(output_file_path, 'a', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		for key in delimiter_parse_time_per_node_map.keys():
			csv_writer.writerow(["PARSER_DELIMITER_TIME", key, '{0:.8f}'.format(average(delimiter_parse_time_per_node_map[key]))])
		for key in materialise_tuple_time_per_node_map.keys():
			csv_writer.writerow(["MATERIALIZE_TUPLE_TIME", key, '{0:.8f}'.format(average(materialise_tuple_time_per_node_map[key]))])

		for key in hdfs_decompression_time_factor_map.keys():
			for inner_key in hdfs_decompression_time_factor_map[key].keys():
				csv_writer.writerow(['HDFS_DECOMPRESSION_TIME_FACTOR', key, inner_key, '{0:.8f}'.format(hdfs_decompression_time_factor_map[key][inner_key])])

		for key in disk_throughput_map.keys():
			for inner_key in disk_throughput_map[key].keys():
				csv_writer.writerow(['DISK_THROUGHPUT', key, inner_key, '{0:.8f}'.format(disk_throughput_map[key][inner_key])])

	return [delimiter_parse_time_per_node_map, materialise_tuple_time_per_node_map, hdfs_decompression_time_factor_map]
		
if __name__ == '__main__':

	import platform
	print(platform.python_version())

	file_format = 'TEXT'
	codec = 'NONE'

	query_name = 'q27.sql.log'
	profile = os.path.join(r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_ALL_TEXT_50\2.7gz', query_name)
	output = r'C:\Development\logs\metrics\scan-stats'
	sys.argv = ['scan_stats.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: scan_stats.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_scanner_stats_info, os.path.normpath(profile), os.path.normpath(output), 'csv')
	