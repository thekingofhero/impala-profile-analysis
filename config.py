#-*- encoding:utf-8 -*-
import os
import platform
def local_config():
    
    profile_path = './log_dir'
    db_type = 'text'
    db_size = '50'
    
    #no change
    #字符串两端可能出现的特殊字符
    char_list = [':',',',']','[','=']
    partitioned_table_name_dic = {
                                  'text':'store_sales_text',
                                  'parquet':'store_sales_parquet',
                                  'parquet_snappy':'store_sales_parquet_snappy',
                                  }
    partitioned_table_name = partitioned_table_name_dic[db_type]
    
    total_table_size_in_bytes_dic = {
                                     '50':18328355990,
                                     '3000':1160538512799
                                 }
    total_table_size_in_bytes = total_table_size_in_bytes_dic[db_size]
    install_dir = os.path.dirname(os.path.realpath(__file__))
    current_sys = platform.system()
    return locals()