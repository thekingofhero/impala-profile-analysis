from scripts.utility import *
import sys
import os
import string
import csv
import re

server_idx_map = {'tracing024':0, 
				  'tracing025':1,
				  'tracing026':2,
				  'tracing027':3}
data_dist = []

def collect_data_dist_info(profile_path, output_file_path, output_dir_path="."):

	average_fragment_metrics_skipped = False
	within_scan_hdfs_entry = False
	within_plan_fragment_entries = False
	
	node_id = 0
	
	records = []
	server_id = 0
	per_node_data_size_in_mb = 0
	scanner_concurrency = 0
	with open(output_file_path, 'w', newline='') as csv_file:
		csv_writer = csv.writer(csv_file, delimiter=',')

		for line in open(profile_path):
			stripped_line = line.strip();

			if stripped_line.startswith("Estimated Per-Host Requirements:"):
				within_plan_fragment_entries = True
			elif stripped_line.startswith("Estimated Per-Host Mem:"):
				within_plan_fragment_entries = False				
			elif stripped_line.startswith("Instance"):
				lft_idx = stripped_line.find("(host=")
				rgh_idx = stripped_line.find(":22000):(")
				host_name = stripped_line[lft_idx+6:rgh_idx]
				server_id = server_idx_map[host_name]
				average_fragment_metrics_skipped = True
			elif stripped_line.startswith("HDFS_SCAN_NODE (id=") and average_fragment_metrics_skipped:
				curr_id = get_exec_node_id(stripped_line)
				within_scan_hdfs_entry = True
			elif average_fragment_metrics_skipped and within_scan_hdfs_entry:

				if stripped_line.startswith("Hdfs split stats"):					
					idx = stripped_line.find(">):")
					stripped_line = stripped_line[idx+4:].strip()
				
					while len(stripped_line) > 0:
						p = re.compile('\d+:\d+/\d+\.\d+ ((GB)|(MB)|(KB)|B)')
						m = p.search(stripped_line)
						start_index = m.span()[0]
						end_index = m.span()[1]

						disk_dist_info = stripped_line[start_index:end_index]
						
						idx = disk_dist_info.find(':')
						volume = disk_dist_info[0:idx]
						disk_dist_info = disk_dist_info[idx+1:]

						idx = disk_dist_info.find('/')
						num_splits = disk_dist_info[0:idx]
						disk_dist_info = disk_dist_info[idx+1:]
						
						data_size_in_mb = get_size_in_mb(disk_dist_info)
						per_node_data_size_in_mb += data_size_in_mb

						stripped_line = stripped_line[end_index:].strip()

						record = [server_id, curr_id, volume, num_splits, "{0:.8f}".format(data_size_in_mb)]
						
						records.append(record)
						data_dist.append(record)

				else:
					key = get_label(stripped_line)
					
					if key == 'TotalReadThroughput':
						within_scan_hdfs_entry = False
					elif key == 'BytesRead':
						bytes_read = get_count(stripped_line)						
						ratio = float(bytes_read)/1024.0/1024.0/per_node_data_size_in_mb		

						for record in records:								
							record.append("{0:.8f}".format(float(record[len(record)-1])*ratio))																	
						per_node_data_size_in_mb = 0

						csv_writer.writerows(records)
						records = []
							
	calculate_scan_range_boundary_ratio()


def calculate_scan_range_boundary_ratio():
	import sys
	global data_dist	
	global server_idx_map

	server_idx = 0
	
	per_server_num_splits_map = {}	
	
	for dd in data_dist:
		
		sid = str(dd[0])
		nid = str(dd[1])
		num_splits = float(dd[3])

		if sid not in per_server_num_splits_map.keys():
			per_server_num_splits_map[sid] = {}
		if nid not in per_server_num_splits_map[sid].keys():
			per_server_num_splits_map[sid][nid] = 0.0
		
		per_server_num_splits_map[sid][nid] += num_splits
	
	per_scan_node_stats = {}
	dist_factor = {}
	per_node_server_idx = {}
	for sid in per_server_num_splits_map.keys():

		nid_count = {}
		for nid in per_server_num_splits_map[sid].keys():

			if nid not in nid_count.keys():
				nid_count[nid] = 1

			if nid not in per_scan_node_stats.keys():
				per_scan_node_stats[nid] = {}
				per_scan_node_stats[nid]['max'] = 0.0
				per_scan_node_stats[nid]['min'] = sys.maxsize
				per_scan_node_stats[nid]['avg'] = 0.0

			if per_scan_node_stats[nid]['max'] < per_server_num_splits_map[sid][nid]:
				per_scan_node_stats[nid]['max'] = per_server_num_splits_map[sid][nid]
				per_node_server_idx[nid] = sid
			
			per_scan_node_stats[nid]['min'] = \
				per_scan_node_stats[nid]['min'] if per_scan_node_stats[nid]['min'] < per_server_num_splits_map[sid][nid] else per_server_num_splits_map[sid][nid]
			per_scan_node_stats[nid]['avg'] += per_server_num_splits_map[sid][nid]
		
		for nid in nid_count.keys():
			if nid not in dist_factor.keys():
				dist_factor[nid] = 0
			dist_factor[nid] += 1


	for nid in per_scan_node_stats.keys():
		per_scan_node_stats[nid]['avg'] /= dist_factor[nid]
		
	for nid in per_scan_node_stats.keys():
		upper_boundary_scan_range_ratio = per_scan_node_stats[nid]['max'] / per_scan_node_stats[nid]['avg']
		lower_boundary_scan_range_ratio = per_scan_node_stats[nid]['min'] / per_scan_node_stats[nid]['avg']
		# print('plan node id {0}'.format(nid))
		# print('<upper_boundary_scan_range_ratio>{0}</upper_boundary_scan_range_ratio>'.format(upper_boundary_scan_range_ratio))
		# print('<lower_boundary_scan_range_ratio>{0}</lower_boundary_scan_range_ratio>'.format(lower_boundary_scan_range_ratio))		
		calculate_disk_splits_boundary_ratio(per_node_server_idx[nid], nid)

	
def calculate_disk_splits_boundary_ratio(server_idx, plan_node_idx):
	global data_dist	
	global server_idx_map

	volume_splits_map = {}
	for dd in data_dist:

		sid = str(dd[0])
		nid = str(dd[1])
		num_splits = float(dd[3])

		if sid == server_idx and nid == plan_node_idx:
			if dd[2] not in volume_splits_map.keys():
				volume_splits_map[dd[2]] = 0
			volume_splits_map[dd[2]] += int(dd[3])
	
	max = 0
	min = sys.maxsize
	avg = 0

	for key in volume_splits_map:
		max = max if max > volume_splits_map[key] else volume_splits_map[key]
		min = min if min < volume_splits_map[key] else volume_splits_map[key]
		avg += volume_splits_map[key]
	avg /= len(volume_splits_map.keys())

#	print('max: {0}, min: {1}, avg: {2}'.format(max, min, avg))
	upper_boundary_disk_splits_ratio = max / avg
	lower_boundary_disk_splits_ratio = min / avg
	# print('<upper_boundary_disk_splits_ratio>{0}</upper_boundary_disk_splits_ratio>'.format(upper_boundary_disk_splits_ratio))
	# print('<lower_boundary_disk_splits_ratio>{0}</lower_boundary_disk_splits_ratio>'.format(lower_boundary_disk_splits_ratio))

if __name__ == '__main__':

	import platform
	print(platform.python_version())

	query_name = 'q19.sql.log'
	profile = os.path.join(r'C:\Users\junliu2\Desktop\1G_2.7G_TEXT_3000', query_name)
	output = r'C:\Development\logs\metrics\data-dist'
	sys.argv = ['data_dist.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: data_dist.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_data_dist_info, os.path.normpath(profile), os.path.normpath(output), 'dist')

	# for dist in data_dist:
	# 	print(dist)
