import os
import sys
import json
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from xml.etree import ElementTree
from xml.dom import minidom

coordinator_log_name = 'impalad.tracing024'

def get_profiles(input_path="."):

	inupt_lists = []

	if os.path.isfile(input_path):
		file_splits = os.path.split(input_path)
		filename = file_splits[1]
		if filename.startswith(coordinator_log_name):
			inupt_lists.append(input_path)
	elif os.path.exists(input_path):
		for subdir, dirs, files in os.walk(input_path):
			for file in files:				
				filename = os.path.split(file)[1]				
				if filename.startswith(coordinator_log_name):
					inupt_lists.append(os.path.join(subdir, file))

	return inupt_lists

def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def gather_slots_info(coordinator_log, output_path):

	tuple_descriptors_element = Element('tuple_descriptors')

	with open(coordinator_log, 'r') as log_file:
		for line in log_file:
			stripped_line = line.strip()
			if stripped_line.startswith('TupleDescriptor{id='):
				
				tuple_descriptor_element = SubElement(tuple_descriptors_element, "tuple_descriptor")

				# tuple id
				stripped_line = stripped_line.replace('TupleDescriptor{id=', '')
				rgh_idx = stripped_line.find(', name=')
				tuple_id = stripped_line[0:rgh_idx]

				# database name and table name
				lft_idx = stripped_line.find('tbl=')
				stripped_line = stripped_line[lft_idx+4:]
				rgh_idx = stripped_line.find(', byte_size')
				qualified_table_name = stripped_line[0: rgh_idx]
				
				# byte size
				lft_idx = stripped_line.find('byte_size=')
				stripped_line = stripped_line[lft_idx+10:]
				rgh_idx = stripped_line.find(', is_materialized')
				byte_size = stripped_line[0:rgh_idx]

				tuple_descriptor_element.set('tuple_id', tuple_id)
				tuple_descriptor_element.set('tbl', qualified_table_name)
				tuple_descriptor_element.set('byte_size', byte_size)
				

				lft_idx = stripped_line.find('slots=[')
				stripped_line = stripped_line[lft_idx+7:]
				
				while True:
					rgh_idx = stripped_line.find(', stats=')

					slot_descriptor = stripped_line[len('SlotDescriptor')+1:rgh_idx]
					slot_descriptor = slot_descriptor.replace('=', '":"')
					slot_descriptor = slot_descriptor.replace(', ', '", "')
					slot_descriptor += '"}'
					slot_descriptor = '{"' + slot_descriptor

					json_sd = json.loads(slot_descriptor)
					slot_id = json_sd['id']
					slot_name = json_sd['col']
					slot_byte_size = json_sd['byteSize']
					slot_byte_offset = json_sd['byteOffset']
					
					slot_element = SubElement(tuple_descriptor_element, 'slot_descriptor')
					slot_element.set('id', slot_id);
					slot_element.set('name', slot_name)
					slot_element.set('byte_size', slot_byte_size)
					slot_element.set('byteOffset', slot_byte_offset)

					rgh_idx = stripped_line.find('SlotDescriptor', rgh_idx)
					if rgh_idx == -1:
						break
					stripped_line = stripped_line[rgh_idx:]
	
	with open(output_path, 'w') as xml_writer:
		xml_writer.write(prettify(tuple_descriptors_element))

if __name__ == '__main__':
	
	coordinator_log_path = r'\\10.239.44.78\home\wangdewei\q98.sql_1440664668\impalad\tuplelog24'
	
	output_path = r'.\output_dir'

	if not os.path.exists(output_path):
		os.makedirs(output_path)

	if os.path.exists(coordinator_log_path):
		if os.path.isdir(coordinator_log_path):
			coordinator_log_list = get_profiles(coordinator_log_path)
			print(coordinator_log_list)			
			for coordinator_log in coordinator_log_list:				
				dirpath = os.path.dirname(coordinator_log)

				basename = os.path.basename(dirpath);
				idx = basename.rfind('sql_')
				if idx != -1:
					basename = basename[0:idx+3]
				tuple_descriptor_filepath = os.path.join(output_path, 'tuple_descriptor_'+basename+'.xml')				
				gather_slots_info(coordinator_log, tuple_descriptor_filepath)
		elif os.path.isfile(coordinator_log_path):
			dirpath = os.path.dirname(coordinator_log_path)
			basename = os.path.basename(dirpath);
			idx = basename.rfind('sql_')
			if idx != -1:
				basename = basename[0:idx+3]
			tuple_descriptor_filepath = os.path.join(output_path, 'tuple_descriptor_'+basename+'.xml')
			gather_slots_info(coordinator_log_path, tuple_descriptor_filepath)		

	