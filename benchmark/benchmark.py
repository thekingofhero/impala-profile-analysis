import sys
import os
import string
import shutil

from benchmark.config_gen import generate_config

def runBenchmark(execution_config_dirpath=r'benchmark\configs',
				 config_template_path=r'benchmark\config.template',
				 config_params_path=r'config.params',
				 execution_directory_path=r'.',
				 log_level='info',
				 log_file_name='benchmark\cofs_execution.log',
				 cof_dp_file='cofs_dp.csv'):

	if os.path.exists(execution_config_dirpath):
		print('remove folder: %s' % execution_config_dirpath)
		shutil.rmtree(execution_config_dirpath)

	generate_config(config_template_path, config_params_path, execution_config_dirpath)

	sim_config_path = os.path.join(execution_directory_path, 'config.xml')
	for subdir, dirs, files in os.walk(execution_config_dirpath):
		for file in files:				
			if file.startswith('config_') and file.endswith('.xml'):
				print(file)
				shutil.move(os.path.join(execution_config_dirpath, file), sim_config_path)
				cmd='''
				ImpalaSimulator.exe --cf-gui-connect=no --cf-gui-time-scale=ns --cf-mon-on-time=\"0.0 us\" --cf-sim-duration=\"1 d\" \
				--cf-verbosity=%s --cf-hpf-enable=no --cf-apf-enable=no  --cf-gui-trace-enable=no \
				--cf-lic-location=28518@plxs0415.pdx.intel.com --cf-log-file=%s  --cf-dp-values-file=\"%s\" 
					''' % (log_level, log_file_name, cof_dp_file)
				print(cmd)
				os.system(cmd)
				filename = file.replace('config_', '').replace('.xml', '')
				if os.path.exists(log_file_name):
					shutil.move(log_file_name, os.path.join('benchmark',''.join([filename, '.log'])))

def runBenchMark(config_params_path,cof_dp_file,block_list):

# 	config_params_path=os.path.join(r'W:\junliu\Benchmark\Impala\Simulator (TEXT)\TEXT_3000_2.7G_10G_8DISK_6N_EST\params', '')
# 	cof_dp_file=os.path.abspath(r'W:\junliu\Benchmark\Impala\Simulator (TEXT)\TEXT_3000_2.7G_10G_8DISK_6N_EST\params\cofs_dp.csv')
# 
# 	block_list = []

	if os.path.isfile(config_params_path):
		if config_params_path.endswith('params') and os.path.splitext(os.path.basename(config_params_path))[0] not in block_list:
			print(config_params_path)
			runBenchmark(config_params_path=config_params_path, cof_dp_file=cof_dp_file) 
	elif os.path.exists(config_params_path):
		for subdir, dirs, files in os.walk(config_params_path):
			for param_file in files:	
				if param_file.endswith('params') and os.path.splitext(os.path.basename(param_file))[0] not in block_list:
					print(os.path.join(config_params_path,param_file))
					runBenchmark(config_params_path=os.path.join(config_params_path,param_file), cof_dp_file=cof_dp_file) 
	