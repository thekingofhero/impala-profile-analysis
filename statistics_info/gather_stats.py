import sys
import os

from scripts.utility import *
from scripts.time_trans import *
from statistics.hj_metrics import collect_statistics as hj_cs
from statistics.aggr_metrics import collect_statistics as aggr_cs
from statistics.exchange import collect_statistics as ex_cs
from statistics.data_stream_sender import collect_statistics as dss_cs
from statistics.scan_stats import collect_scanner_stats_info as scan_cs
from statistics.data_dist import collect_data_dist_info, calculate_scan_range_boundary_ratio

def get_time(profile_path, output_file_path, output_dir_path="."):

	total_time = 0.0
	execution_time = 0.0
	remote_fargment_started = 0.0

	for	line in open(profile_path):
		if line.startswith('Fetched'):
			start_idx = line.find('in ')
			end_idx = len(line)

			time_in_str = line[start_idx+3:end_idx].strip()
			total_time = float(get_time_in_str_ns(time_in_str)) / 1000000000
		elif line.strip().startswith('- Remote fragments started'):
			start_idx = line.find(': ')
			end_idx = line.find(' (')
			
			time_in_str = line[start_idx+2:end_idx].strip()
			remote_fargment_started = float(get_time_in_str_ns(time_in_str)) / 1000000000
	execution_time = total_time - remote_fargment_started

	print(total_time, remote_fargment_started, execution_time)

if __name__ == '__main__':

	# import platform
	# print('Python Version {0}'.format(platform.python_version()))

	query_name = 'ss_max.sql.log'
	profile = os.path.join(r'C:\Users\junliu2\Syncplicity\Benchmarks\1G_2.7G_PARQUET_50\plan', query_name)
	output = os.path.join(r'C:\Development\logs\metrics\gather-stats', query_name)
	sys.argv = ['gather-stats.py', profile]

	if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
		print("usage: gather_stats.py <impala profile file path or directory>")
		sys.exit()

	handle_profiles(collect_data_dist_info, os.path.normpath(profile), os.path.normpath(output), 'dist')
	# handle_profiles(get_time, os.path.normpath(profile), os.path.normpath(output))
	handle_profiles(hj_cs, os.path.normpath(profile), os.path.normpath(output))
	handle_profiles(aggr_cs, os.path.normpath(profile), os.path.normpath(output))
	handle_profiles(ex_cs, os.path.normpath(profile), os.path.normpath(output))
	handle_profiles(dss_cs, os.path.normpath(profile), os.path.normpath(output))
	handle_profiles(scan_cs, os.path.normpath(profile), os.path.normpath(output), 'csv')

