import sys
import os
import string

from string import Template

template_param_index = -1

def inc_and_get():
	global template_param_index 
	template_param_index += 1
	return template_param_index


def generate_config(template_file_path, params_file_path, output_dir_path):

	global template_param_index 

	print('template file path: %s' % template_file_path)
	print('params file path: %s' % params_file_path)

	config_output_dir = os.path.join(output_dir_path, 'configs')

	if not os.path.exists(config_output_dir):
		os.makedirs(config_output_dir)

	# $enable_firespring_poc ......... index 0
	# $enable_remote_cache ........... index 1
    # $enable_hdfs_cache ............. index 2
	# $pert_hits ..................... index 3
	# $execution_plan_location ....... index 4
	# $execution_plan_name ........... index 5
	# $execution_plan_statistics ..... index 6
	# $data_distribution ............. index 7
	# $metadata_file_path ............ index 8
	# $tuple_descriptor_file_path .... index 9
	# $network_clock_interval ........ index 10
	

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
		with open(params_file_path, 'r', newline='') as params_reader:
			for line in params_reader:

				template_param_index = -1

				line = line.strip()

				if line.startswith('#') or len(line.strip()) == 0:
					continue
				
				splits = line.split('|')
				
				param_enable_firespring_poc=splits[inc_and_get()]
				param_enable_remote_cache=splits[inc_and_get()]
				param_enable_hdfs_cache=splits[inc_and_get()]
				param_pert_hits=splits[inc_and_get()]
				param_execution_plan_location=splits[inc_and_get()]
				param_execution_plan_name=splits[inc_and_get()]
				param_execution_plan_statistics=splits[inc_and_get()]
				param_data_distribution=splits[inc_and_get()]
				param_metadata_file_path=splits[inc_and_get()]
				param_tuple_descriptor_file_path=splits[inc_and_get()]
				param_network_clock_interval=splits[inc_and_get()]
				

				new_template = config_template.substitute(
								enable_firespring_poc=param_enable_firespring_poc,
								enable_remote_cache=param_enable_remote_cache,
								enable_hdfs_cache=param_enable_hdfs_cache,
								pert_hits=param_pert_hits,
								execution_plan_location=param_execution_plan_location,
								execution_plan_name=param_execution_plan_name,
								execution_plan_statistics=param_execution_plan_statistics,
								data_distribution=param_data_distribution,
								metadata_file_path=param_metadata_file_path,
								tuple_descriptor_file_path=param_tuple_descriptor_file_path,
								network_clock_interval=param_network_clock_interval)

				query_name = os.path.splitext(param_execution_plan_name)[0]
				output_file_name = '_'.join(['config', query_name, str(output_file_name_index)])
				output_file_name = '.'.join([output_file_name, 'xml'])
				output_file_name = os.path.join(config_output_dir, output_file_name)
				output_file_name_index += 1

				with open(output_file_name, 'w+') as config_writer:
					config_writer.write(new_template)
	else:
		print("cannot find path %s" % params_file_path)


if __name__ == '__main__':

	# sys.argv = ['conf_gen.py', 
	# 			r'config.template', 
	# 		    r'C:\Users\junliu2\Syncplicity\Benchmarks\firespring\simulator\firespring.params', 
	# 		    r'C:\Users\junliu2\Syncplicity\Benchmarks\firespring\simulator']

	if len(sys.argv) < 3:
		print("usage: config_template.py <template file> <params file> <output_dir_path")
		sys.exit()

	generate_config(sys.argv[1], sys.argv[2], sys.argv[3])