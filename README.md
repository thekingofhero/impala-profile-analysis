## impala-profile-analysis
impala-profile-analysis
#1.输入：<br> 
　　　①工程根目录创建 config.conf文件：<br> 
　　　　　##[Extract simulator's input from queries' profile]<br> 
　　　　　#switch_input=True<br> 
　　　　　#profile_path=w:\workspace\impala_pt\queries\logs\fastest_logs\2.7<br> 
　　　　　#db_type=parquet<br> 
　　　　　#total_table_size_in_bytes=31522384490 #parquet_150<br> 
　　　　　##[Extract tuple descriptor from implalad log]<br> 
　　　　　#switch_tuple = False<br> 
　　　　　#impaladlog_path=w:\workspace\impala_pt\queries\logs\fastest_logs\2.7<br> 

　　　　switch_*：switch开头的变量是开关，表示该段落是否被加载<br> 
　　profile_path:profile文件存放的路径，既可为文件夹，也可以是单个profile文件<br> 
　　impaladlog_path:impala守护进程的log路径，用于生成tuple_descriptor_*.xml<br> 
　　db_type:text/parquet/parquet_snappy(以后可以修改为db_type and compresion_type)<br> 
　　total_table_size_in_bytes: store_sales_*数据表大小，用字节表示，目前已经统计出来一部分，根据需要使用。<br> 
　　config_params_path ：生成config.xml的参数文件的路径<br> 

#2.输出：<br> 
　　output_dir 下获取执行计划*.xml/*.dist /*.statistic/*.hbase.dist<br> 



    
