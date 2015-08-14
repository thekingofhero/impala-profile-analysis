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
from statistics import *

def collect_statistics(profile_path, output_file_path, output_dir_path="."):

	keys = ['id',                          # index 0
			'BytesSent',                   # index 1
            'NetworkThroughput(*)',        # index 2
           	'OverallThroughput',           # index 3
           	'PeakMemoryUsage',             # index 4           	 
           	'SerializeBatchTime',          # index 5
           	'ThriftTransmitTime(*)',       # index 6
           	'UncompressedRowBatchSize'     # index 7
		    ]

	func_map = {}
	func_map['BytesSent'] = get_count
	func_map['NetworkThroughput(*)'] = get_count
	func_map['OverallThroughput'] = get_count
	func_map['PeakMemoryUsage'] = get_count
	func_map['SerializeBatchTime'] = get_time
	func_map['ThriftTransmitTime(*)'] = get_count
	func_map['UncompressedRowBatchSize'] = get_count
	
	record = []
	
	average_fragment_metrics_skipped = False
	within_data_stream_sender_entry = False
	within_plan_fragment_entries = False
	
	node_id = 0
	geomean_serialization_bytes_per_ns = 1.0
	geomean_decompression_ratio = 1.0

	sample_decompression_ratio = []
	sample_serialization_bytes_per_ns = []

	decompression_ratio_per_node_map = {}
	serialization_bytes_per_ns_per_node_map = {}

	sample_set = [0.0]*4

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
			elif stripped_line.startswith("Instance") and not average_fragment_metrics_skipped:
				average_fragment_metrics_skipped = True
			elif stripped_line.startswith("DataStreamSender (dst_id=") and average_fragment_metrics_skipped:
				curr_id = get_exec_node_id(stripped_line)
				record.append(curr_id)
				within_data_stream_sender_entry = True
			elif average_fragment_metrics_skipped and within_data_stream_sender_entry:

				key = get_label(stripped_line)
				
				if key != '' and key in func_map:
					value = func_map[key](stripped_line)
					record.append(value)

					if key == 'UncompressedRowBatchSize':
						
						serialization_bytes_per_ns = float(record[1]) / float(record[5])
						record.append(serialization_bytes_per_ns)
						sample_serialization_bytes_per_ns.append(serialization_bytes_per_ns)
						sample_set[0] += float(record[1])
						sample_set[1] += float(record[5])

						if curr_id not in serialization_bytes_per_ns_per_node_map.keys():
							serialization_bytes_per_ns_per_node_map[curr_id] = []
						serialization_bytes_per_ns_per_node_map[curr_id].append(serialization_bytes_per_ns)

						decompression_ratio = float(record[7]) / float(record[1]) 
						record.append(decompression_ratio)					
						sample_decompression_ratio.append(decompression_ratio)
						sample_set[2] += float(record[7])
						sample_set[3] += float(record[1])

						if curr_id not in decompression_ratio_per_node_map.keys():
							decompression_ratio_per_node_map[curr_id] = []
						decompression_ratio_per_node_map[curr_id].append(decompression_ratio)

						within_data_stream_sender_entry = False
						
						csv_writer.writerows([record])
						record = []
						
			# enf if
		# end for		

		geomean_serialization_bytes_per_ns = stdev(sample_serialization_bytes_per_ns)
		geomean_decompression_ratio = stdev(sample_decompression_ratio)

		record = [''] * 13
		record.append(geomean_serialization_bytes_per_ns)
		record.append(geomean_decompression_ratio)

		csv_writer.writerows([record])
	# end with
	
	agm_geomean_serialization_bytes_per_ns = agm(geomean_serialization_bytes_per_ns, stdev(sample_serialization_bytes_per_ns))
	agm_geomean_decompression_ratio = agm(geomean_decompression_ratio, stdev(sample_decompression_ratio))

	# print('<row_batch_serialization_cost_bytes_per_ns>{0}</row_batch_serialization_cost_bytes_per_ns>'.format(sample_set[0]/sample_set[1]))
	# print('<sender_data_decompression_ratio>{0}</sender_data_decompression_ratio>'.format(sample_set[2]/sample_set[3]))
	
	for key in serialization_bytes_per_ns_per_node_map.keys():
		print('ROW_BATCH_SER_COST,{0},{1}'.format(key, average(serialization_bytes_per_ns_per_node_map[key])))	
	for key in decompression_ratio_per_node_map.keys():
		print('DECOMPRESSION_RATIO,{0},{1}'.format(key, average(decompression_ratio_per_node_map[key])))	

if __name__ == '__main__':

	import platform
	print(platform.python_version())

	# un-comment the following two lins if you want to run the script through an IDE/Python Editor
	# profile = os.path.join('.', 'q98.sql_1427955243', 'profiles', 'q98.sql.log')
	profile = r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_2.7G_PARQUET_50\q52.sql.log'
	output = r'C:\Development\logs\metrics\data_stream_sender'
	sys.argv = ['data_stream_sender.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: data_stream_sender.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_statistics, os.path.normpath(profile), os.path.normpath(output))
