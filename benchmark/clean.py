
import sys
import os
import string
import shutil
import argparse

def cleanup(dry_run=False):

	generated_config_dir_path = r'./configs'
	current_path = r'.'

	if os.path.exists(generated_config_dir_path) and os.path.isdir(generated_config_dir_path):
		print('remove folder: %s' % generated_config_dir_path)
		if not dry_run:
			shutil.rmtree(generated_config_dir_path)

	'''
		Remove all log files and auto-generated config files from the current (and sub) directories
	'''
	for subdir, dirs, files in os.walk(current_path):
		for target_file in files:
			if target_file.endswith('.log') or target_file == 'config.xml':
				print('remove file: %s' % target_file)
				if not dry_run:
					os.remove(target_file)

				

if __name__ == '__main__':

	sys.argv = ['clearnup.py', '--dry-run']

	parser = argparse.ArgumentParser(description='Simulator Clearnup Script')
	parser.add_argument('--dry-run', dest='enable_dry_run', action='store_true', help='enable dry run (default: false)')
	
	args = parser.parse_args()
	print('enable dry run -> %s' % args.enable_dry_run)

	cleanup() 