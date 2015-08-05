import sys
import os
from string import Template

def generate_config(template_file_path, params_file_path):
	print('template file path: %s' % template_file_path)
	print('params file path: %s' % params_file_path)

	config_output_dir = 'configs'

	if not os.path.exists(config_output_dir):
		os.mkdir(config_output_dir)

	# $execution_plan_location ....... index 0
	# $execution_plan_name ........... index 1
	# $metadata_file_path ............ index 2
	# $tuple_descriptor_file_path .... index 3

	str_template = ''
	
	if os.path.isfile(template_file_path):
		with open(template_file_path, 'r', newline='') as template_reader:
			for line in template_reader:
				str_template += line.strip()
	else:
		print("cannot find path %s" % template_file_path)

	if os.path.isfile(params_file_path):
		config_template = Template(str_template)
		with open(params_file_path, 'r', newline='') as params_reader:
			for line in params_reader:
				line = line.strip()

				if line.startswith('#'):
					continue

				splits = line.split('|')
				
				if len(splits) == 8:
					new_template = config_template.substitute(
									execution_plan_location=splits[0],
									execution_plan_name=splits[1],
									metadata_file_path=splits[2],
									tuple_descriptor_file_path=splits[3],
									query_executor_execution_granularity_ns=splits[4],
									disk_scan_granularity_ns=splits[5],
									read_range_granularity_ns=splits[6],
									scanner_granularity_ns=splits[7])

					output_file_name = os.path.join(config_output_dir, splits[1])
					with open(output_file_name, 'w+') as config_writer:
						config_writer.write(new_template)
	else:
		print("cannot find path %s" % params_file_path)


if __name__ == '__main__':

	#sys.argv = ['conf_gen.py', r'config.template', r'config.params']

	if len(sys.argv) < 3:
		print("usage: config_template.py <template file> <params file>")
		sys.exit()

	generate_config(sys.argv[1], sys.argv[2])