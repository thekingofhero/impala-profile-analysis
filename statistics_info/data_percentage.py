import sys
import os
import string
import re
import csv


from scripts.utility import get_size_in_bytes
from scripts.utility import get_node_id
from scripts.utility import get_partition_size

def collect_data_percentage(partitioned_table_name, total_table_size_in_bytes, profile_path, plan_name):
	# print('profile path [%s], plan name [%s]' % (profile_path, plan_name))

	within_scan_hdfs = False
	node_id = ''

	
	with open(profile_path, 'r', newline='\n') as profile_reader:
		for line in profile_reader:
			line = line.strip()			
			if line.find(":SCAN HDFS [") != -1 and line.find(partitioned_table_name) != -1:
				within_scan_hdfs = True
				node_id = get_node_id(line)
			elif within_scan_hdfs and line.startswith("partitions="):
				
				partition_info = get_partition_size(line)

				actual_partition_size = int(partition_info[0])
				total_partition_size = int(partition_info[1])

				index = line.find("size=")
				size_in_bytes = 0
				data_percentage = 0.0

				if actual_partition_size == total_partition_size:
					size_in_bytes = total_table_size_in_bytes
					data_percentage = 1.0
				elif index != -1:
					size_in_str = line[index+len("size="):]					
					size_in_bytes = get_size_in_bytes(size_in_str)
					data_percentage = size_in_bytes / total_table_size_in_bytes
				
				print('plan name: %s, node id: %s, total size: %f, data size: %f, percentage: %f' % (plan_name, node_id, size_in_bytes, total_table_size_in_bytes, data_percentage))
				within_scan_hdfs = False
				node_id = ''
			elif line.startswith("Estimated Per-Host Mem"):
				return
			


def handle_profiles(input_path="."):

	inupt_lists = []

	if os.path.isfile(input_path):
		file_splits = os.path.split(input_path)
		filename = file_splits[1]
		filename = os.path.splitext(filename)[0]
		filename = filename + ".xml"
		inupt_lists.append([input_path, filename])
	elif os.path.exists(input_path):
		for subdir, dirs, files in os.walk(input_path):
			for file in files:				
				if file.endswith(".log"):
					filename = os.path.split(file)[1]
					filename = os.path.splitext(filename)[0]
					filename = filename + ".xml"
					inupt_lists.append([os.path.join(subdir, file), filename])

	return inupt_lists

if __name__ == '__main__':
	import platform
	print(platform.python_version())
	
	query_name = ''
	profile = os.path.join(r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_2.7G_TEXT_3000', query_name)

	sys.argv = ['data_percentage.py', profile]

	inupt_lists = handle_profiles(os.path.normpath(profile))
	for input_list in inupt_lists:
		# collect_data_percentage("store_sales_parquet", 11076012572, input_list[0], input_list[1])
		# collect_data_percentage("store_sales_text", 18328355990, input_list[0], input_list[1])
		
		# 3000
		collect_data_percentage("store_sales_text", 1160538512799, input_list[0], input_list[1])
		
	