#-*- encoding:utf-8 -*-
import os
def local_config():
    #字符串两端可能出现的特殊字符
    char_list = [':',',',']','[','=']
    profile_path = './log_dir'
    db_type = 'text'
    db_size = '50'
    
    #no change
    partitioned_table_name_dic = {
                                  'text':'store_sales_text',
                                  'parquet':'store_sales_parquet',
                                  'parquet_snappy':'store_sales_parquet_snappy',
                                  }
    partitioned_table_name = partitioned_table_name_dic[db_type]
    
    total_table_size_in_bytes_dic = {
                                     '50':1160538512799,
                                     '3000':1160538512799
                                 }
    total_table_size_in_bytes = total_table_size_in_bytes_dic[db_size]
    install_dir = os.path.dirname(os.path.realpath(__file__))
    return locals()