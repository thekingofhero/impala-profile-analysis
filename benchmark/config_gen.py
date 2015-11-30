import sys
import os
import string

from string import Template
import time
current_milli_time = lambda: int(round(time.time() * 1000))


def generate_config(template_file_path, params_file_path, output_dir_path):

	print('template file path: %s' % template_file_path)
	print('params file path: %s' % params_file_path)

	config_output_dir = os.path.join(output_dir_path)

	if not os.path.exists(config_output_dir):
		os.makedirs(config_output_dir)

	str_template = ''
	
	if os.path.isfile(template_file_path):
		with open(template_file_path, 'r', newline='') as template_reader:
			for line in template_reader:
				str_template += line.strip()
	else:
		print("cannot find path %s" % template_file_path)

	output_file_name_index = 0
	if os.path.isfile(params_file_path):
		config_template = Template(str_template)

		param_map = {
			'file_format':'n/a',
			'scaling_factor':'n/a',
			'query_name':'n/a',
			'compression_codec':'n/a',
			'enable_data_distribution_estimation':'false',
			'execution_plan_location':'n/a',
			'execution_plan_name':'n/a',
			'execution_plan_statistics':'n/a',
			'data_distribution':'n/a',
			'data_distribution_estimation':'n/a',
			'metadata_file_path':'n/a',
			'tuple_descriptor_file_path':'n/a',
			'disk_model_type':'-1',
			'built_in_disk_model_file':'n/a',
			'enable_firespring_poc':'false',
			'enable_remote_cache':'false',
			'enable_hdfs_cache':'false',
			'pert_hits':'0',
			'network_clock_interval':'-1',
			'num_data_disks':'-1',
			'total_num_disks':'-1',
			'default_disk_speed_in_mb':'-1',
			'proc_perf_indicator':'1',
			'server_num':'-1'
		}

		with open(params_file_path, 'r', newline='') as params_reader:
			for line in params_reader:

				splits = line.strip().split('=')
				key = splits[0]
				value = splits[1]

				param_map[key] = value

# 			for k,v in param_map.items():
# 				print("param(\'{key}\') = {value}".format(key=k,value=v))

			max_row_batch_size=param_map['total_num_disks'] if 'total_num_disks' in param_map.keys() else '-1'
			max_row_batch_size=str(int(max_row_batch_size)*10)

			new_template = config_template.substitute(
							file_format=param_map['file_format'],
							scaling_factor=param_map['scaling_factor'],
							query_name=param_map['query_name'],
							compression_codec=param_map['compression_codec'],
							enable_data_distribution_estimation=param_map['enable_data_distribution_estimation'],
							execution_plan_location=param_map['execution_plan_location'],
							execution_plan_name=param_map['execution_plan_name'],
							execution_plan_statistics=param_map['execution_plan_statistics'],
							data_distribution=param_map['data_distribution'],
							data_distribution_estimation=param_map['data_distribution_estimation'],
							metadata_file_path=param_map['metadata_file_path'],
							tuple_descriptor_file_path=param_map['tuple_descriptor_file_path'],
							disk_model_type=param_map['disk_model_type'],
							built_in_disk_model_file=param_map['built_in_disk_model_file'],
							enable_firespring_poc=param_map['enable_firespring_poc'],
							enable_remote_cache=param_map['enable_remote_cache'],
							enable_hdfs_cache=param_map['enable_hdfs_cache'],
							pert_hits=param_map['pert_hits'],
							network_clock_interval=param_map['network_clock_interval'],
							DiskNum=param_map['num_data_disks'],							
							max_row_batch_size=max_row_batch_size,
							default_disk_speed_in_mb=param_map['default_disk_speed_in_mb'],
							proc_perf_indicator=param_map['proc_perf_indicator'],
							server_num=param_map['server_num']
							)

			
			query_name = param_map['query_name']
			output_file_name = '_'.join(['config', 
										 query_name, 
										 param_map['file_format'], 
										 param_map['compression_codec'], 
										 param_map['scaling_factor'], 
										 param_map['network_clock_interval'],
										 str(current_milli_time())]
									)
			output_file_name = output_file_name.replace('.', '_')
			output_file_name = '.'.join([output_file_name, 'xml'])
			output_file_name = os.path.join(config_output_dir, output_file_name)
			
			print('output file name %s' % (output_file_name))
			with open(output_file_name, 'w+') as config_writer:
				config_writer.write(new_template)
	else:
		print("cannot find path %s" % params_file_path)


if __name__ == '__main__':

	# sys.argv = ['conf_gen.py', 
	# 			r'config.template', 
	# 		    os.path.join('params', 'TEXT_SF=3000', 'q59.params'), 
	# 		    r'configs']

	if len(sys.argv) < 3:
		print("usage: config_template.py <template file> <params file> <output_dir_path")
		sys.exit()

	generate_config(sys.argv[1], sys.argv[2], sys.argv[3])