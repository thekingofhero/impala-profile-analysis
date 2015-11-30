#-*- encoding:utf-8 -*-
import os
import platform
def local_config():
    block_list = []
    with open(".\config.conf",'r') as fp:
        for line in fp:
            if line.startswith("#"):
                continue
            if "#" in line:
                line = line[0:line.find("#")]
                
            if line.startswith("switch_input"):
                sw_input = str(True) == line.split("=")[1].strip()   
            if line.startswith("profile_path"):
                profile_path = line.split("=")[1].strip()
            if line.startswith("impaladlog_path"):
                impaladlog_path=line.split("=")[1].strip()
            if line.startswith("db_type"):
                db_type = line.split("=")[1].strip()
            if line.startswith("total_table_size_in_bytes"):
                total_table_size_in_bytes=int(line.split("=")[1].strip())
                
            if line.startswith("switch_tuple"):
                sw_tuple = str(True) == line.split("=")[1].strip()
            if line.startswith("switch_runBenchmark"):
                sw_runBenchmark= str(True) == line.split("=")[1].strip()
                
            if line.startswith("config_params_path"):
                config_params_path=str(line.split("=")[1].strip())
            if line.startswith("cof_dp_file"):
                cof_dp_file=str(line.split("=")[1].strip())
            if line.startswith("block_list"):
                block_list.append(line.split("=")[1].strip().strip("'"))
    server_idx_map = {'tracing024':0, 
                  'tracing025':1,
                  'tracing026':2,
                  'tracing027':3,
                  'tracing022':4,
                  'tracing016':5,
                  'tracing020':6}
    #no change
    #字符串两端可能出现的特殊字符
    char_list = [':',',',']','[','=']
    partitioned_table_name_dic = {
                                  'text':'store_sales_text',
                                  'parquet':'store_sales_parquet',
                                  'parquet_snappy':'store_sales_parquet_snappy',
                                  }
    partitioned_table_name = partitioned_table_name_dic[db_type]
    install_dir = os.path.dirname(os.path.realpath(__file__))
    current_sys = platform.system()
    for check_item in ['profile_path','db_type','total_table_size_in_bytes','impaladlog_path']:
        #print(check_item)
        assert check_item in locals()
    return locals()

if __name__ =='__main__':
    local_config()