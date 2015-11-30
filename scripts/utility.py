def maximum(value_list):
	if len(value_list) == 0:
		return 0

	max = 0.0
	for v in value_list:
		max = v if v > max else max

	return max

def average(value_list):
	if len(value_list) == 0:
		return 0

	sum = 0.0
	for v in value_list:
		sum += v

	return sum/len(value_list)

def geomean(value_list):

	if len(value_list) == 0:
		return 0

	product = 1
	for v in value_list:
		product *= v
	return product ** (1/len(value_list))

def agm(a, g, depth=0, limit=900):	
	if a == g or depth == limit:
		return a
	else:
		tmp_a = (a+g)/2
		tmp_g = geomean([a,g])
		return agm(tmp_a, tmp_g, depth+1, limit)

# if __name__ == '__main__':
# 	print(arithmatic_geomean(24, 6))



def handle_profiles(fn_ref, input_path=".", output_dir_path=".", output_file_type="csv"):
	import os
	if not os.path.exists(output_dir_path):
		os.makedirs(output_dir_path)
	if os.path.isfile(input_path):
		file_splits = os.path.split(input_path)
		filename = file_splits[1]
		output_file_path = os.path.join(output_dir_path, get_output_name(filename, output_file_type))
		fn_ref(input_path, output_file_path, output_dir_path);
		
	elif os.path.exists(input_path):
		for subdir, dirs, files in os.walk(input_path):
			for file in files:				
				if file.endswith(".log"):
					fn_ref(os.path.join(subdir, file), os.path.join(output_dir_path, get_output_name(file, output_file_type)), output_dir_path);

def get_output_name(profile_name, output_file_type):
	import time
	#millis = int(round(time.time() * 1000))
	#output_file_name = profile_name + '_' + str(millis) + '.' + output_file_type
	profile_name = profile_name.replace(".sql.log", "")
	output_file_name = profile_name + '.' + output_file_type
	return output_file_name

def get_time_by_unit(number, unit):
	if unit == 'us':
		return number * 1000
	elif unit == 'ms':
		return number * 1000000
	elif unit == 'ns':
		return number
	elif unit == 'm':
		return number * 1000000000 * 60
	else: #second
		return number * 1000000000

def get_time_in_str_ns(time_in_str):
	time = 0
	number = ''
	
	for index in range(len(time_in_str)):
		
		if (time_in_str[index] == 's' and not time_in_str[index-1].isdigit()):
			continue

		if time_in_str[index].isdigit() or time_in_str[index] == '.':
			number += time_in_str[index]
			if number.startswith('0') and len(number) > 1:
				number = number[1:]
		elif time_in_str[index] == 'u' and time_in_str[index+1] == 's':
			time += get_time_by_unit(float(number), 'us')
			number = ''
		elif time_in_str[index] == 'n' and time_in_str[index+1] == 's':
			time += get_time_by_unit(float(number), 'ns')
			number = ''
		elif time_in_str[index] == 'm' and (index+1 == len(time_in_str) or time_in_str[index+1] != 's'):
		 	time += get_time_by_unit(float(number), 'm')
		 	number = ''
		elif time_in_str[index] == 'm' and time_in_str[index+1] == 's':
			time += get_time_by_unit(float(number), 'ms')
			number = ''		
		elif time_in_str[index] == 's' and (index+1 == len(time_in_str) or time_in_str[index+1] != 's'):
			time += get_time_by_unit(float(number), 's')		
			number = ''	
	return str(time)

def get_non_child_time_in_str(stripped_line):

	start_idx = stripped_line.find('non-child:')
	end_idx = stripped_line.find(', % non-child:')

	if start_idx != -1 and end_idx != -1:
		return get_time_in_str_ns(stripped_line[start_idx+10:end_idx])


def get_time(stripped_line):	
	
	start_idx = stripped_line.find(': ')
	end_idx = stripped_line.find(' (')
	if end_idx == -1:
		end_idx = len(stripped_line)

	tmp = stripped_line[start_idx+2:end_idx].strip()
	
	return get_time_in_str_ns(tmp)

def get_count(stripped_line):
	tmp_lb_idx = stripped_line.find('(')
	if tmp_lb_idx > 0:
		tmp_rb_idx = stripped_line.find(')')					
		tmp = stripped_line[tmp_lb_idx+1:tmp_rb_idx]					
	else:
		tmp_lb_idx = stripped_line.find(': ')
		tmp = stripped_line[tmp_lb_idx+2:]
	return tmp

def get_row_return_rate(stripped_line):
	tmp_lb_idx = stripped_line.find(': ')
	tmp = stripped_line[tmp_lb_idx+2:]
	if tmp == '0':
		return tmp

	splits = tmp.split("/")
	rate_str = splits[0].strip()
	time_unit = splits[1].strip()
	
	row_per_sec = 0.0
	if rate_str.endswith('K'):
		row_per_sec = float(rate_str.replace('K', '').strip()) * 1000
	elif rate_str.endswith('M'):
		row_per_sec = float(rate_str.replace('M', '').strip()) * 1000000
	else:
		row_per_sec = float(rate_str.strip())

	return str(row_per_sec)

def get_exec_node_id(stripped_line):
	
	end_idx = stripped_line.find(')')
	start_idx = stripped_line.find('(id=')
	if start_idx == -1:
		start_idx = stripped_line.find('(dst_id=')
		return stripped_line[start_idx+8:end_idx].strip()
	return stripped_line[start_idx+4:end_idx].strip()

def get_total_time(stripped_line):

	if stripped_line is None:
		return

	start_idx = stripped_line.find('Total:')

	if start_idx != -1:
		end_idx = stripped_line.find(', non-child')
		
		time_in_str = stripped_line[start_idx+5:end_idx].strip()
		return float(get_time(time_in_str))
	else:
		print('cannot find \'Total\'')

	return 0


def get_label(stripped_line, start_st='- ', end_str=': '):

	label = ''

	lft_idx = stripped_line.find('- ')
	rgt_idx = stripped_line.find(': ')

	if lft_idx != -1 and rgt_idx != -1:
		label = stripped_line[lft_idx+2:rgt_idx]
	elif rgt_idx != -1:
		label = stripped_line[0:rgt_idx]	

	return label

def get_node_id(stripped_line):
	label = ''
	rgt_idx = stripped_line.find(':')
	if rgt_idx != -1:
		label = stripped_line[0: rgt_idx]
	return label

def get_size_in_bytes_by_unit(size, unit):
	if unit == 'gb':
		return size * 1024 * 1024 * 1024
	elif unit == 'mb':
		return size * 1024 * 1024
	elif unit == 'kb':
		return size * 1024
	else: #second
		return size

def get_size_in_bytes(size_with_unit_in_str):
	import re

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(size_with_unit_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	size_in_str = size_with_unit_in_str[start_index: end_index]
	unit_in_str = size_with_unit_in_str[end_index:]
	
	return get_size_in_bytes_by_unit(float(size_in_str), unit_in_str.lower())

def get_size_in_mb_by_unit(size, unit):
	
	if unit == 'gb':
		return size * 1024.0
	elif unit == 'b':
		return size / 1024.0 / 1024.0
	elif unit == 'kb':
		return size / 1024.0
	else:
		return size

def get_size_in_mb(size_with_unit_in_str):
	import re

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(size_with_unit_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	size_in_str = size_with_unit_in_str[start_index: end_index].strip()
	unit_in_str = size_with_unit_in_str[end_index:].strip()

	return get_size_in_mb_by_unit(float(size_in_str), unit_in_str.lower())

def get_throughput_in_mb(stripped_line):
	import re

	throughput_in_str = ''
	idx = stripped_line.find(':')
	if idx != -1:
		throughput_in_str = stripped_line[idx+1:].strip()

	p = re.compile('\d+(\.\d+)?', re.IGNORECASE)
	m = p.match(throughput_in_str)
	start_index = m.span()[0]
	end_index = m.span()[1]

	unit_in_str = throughput_in_str[end_index+1:].lower()
	throughput_in_str = throughput_in_str[start_index:end_index]
	if float(throughput_in_str) == 0:
		return 0.0	

	idx = unit_in_str.find("/");
	size_in_str = unit_in_str[0:idx]
	time_in_str = unit_in_str[idx+1:]

	time = get_time_by_unit(1, time_in_str)
	throughput = get_throughput_in_mb_by_unit(float(throughput_in_str), size_in_str)

	return float(throughput) * float(time)


def get_throughput_in_mb_by_unit(throughput, unit):

	if unit == 'gb':
		return throughput * 1024.0
	elif unit == 'b':
		return throughput / 1024.0 / 1024.0
	elif unit == 'kb':
		return throughput / 1024.0
	else:
		return throughput

def get_partition_size(stripped_line):
	
	partition_size = []

	lft_idx = stripped_line.find('partitions=')
	rgt_idx = stripped_line.find(' files=')

	if lft_idx != -1 and rgt_idx != -1:
		partition_info = stripped_line[lft_idx+len('partitions='):rgt_idx]
		partition_size = partition_info.split("/")

	return partition_size