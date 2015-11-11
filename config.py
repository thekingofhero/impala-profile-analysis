#-*- encoding:utf-8 -*-
import os
import platform
def local_config():
# config.conf's content:
# profile_path=\\10.239.44.78\home\wangdewei\workspace\impala_pt\queries\logs\DATE2015-08-28_TIME16-32\fastest_logs\2.7
# impaladlog_path=\\10.239.44.78\home\wangdewei\workspace\impala_pt\queries\logs\DATE2015-08-28_TIME16-32\fastest_logs\2.7
# db_type=text
# total_table_size_in_bytes=1160538512799#text_3000
# #total_table_size_in_bytes=763065089970#parquet_3000
# #total_table_size_in_bytes=11073840767#parquet_50
# #total_table_size_in_bytes=18328355990#text_50
    with open(".\config.conf",'r') as fp:
        for line in fp:
            if line.startswith("profile_path"):
                profile_path = line.split("=")[1].strip()
            if line.startswith("impaladlog_path"):
                impaladlog_path=line.split("=")[1].strip()
            if line.startswith("db_type"):
                db_type = line.split("=")[1].strip()
            if line.startswith("total_table_size_in_bytes"):
                total_table_size_in_bytes=int(line.split("=")[1].strip())
    
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