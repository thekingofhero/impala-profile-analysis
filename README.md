# impala-profile-analysis
impala-profile-analysis
1.输入：
	①工程根目录创建 config.conf文件：
		##[Extract simulator's input from queries' profile]
		#switch_input=True
		#profile_path=w:\workspace\impala_pt\queries\logs\fastest_logs\2.7
		#db_type=parquet
		#total_table_size_in_bytes=31522384490 #parquet_150
		##[Extract tuple descriptor from implalad log]
		#switch_tuple = False
		#impaladlog_path=w:\workspace\impala_pt\queries\logs\fastest_logs\2.7

	switch_*：switch开头的变量是开关，表示该段落是否被加载
    profile_path:profile文件存放的路径，既可为文件夹，也可以是单个profile文件
    impaladlog_path:impala守护进程的log路径，用于生成tuple_descriptor_*.xml
    db_type:text/parquet/parquet_snappy(以后可以修改为db_type and compresion_type)
    total_table_size_in_bytes: store_sales_*数据表大小，用字节表示，目前已经统计出来一部分，根据需要使用。
	config_params_path ：生成config.xml的参数文件的路径

2.输出：
	output_dir 下获取执行计划*.xml/*.dist /*.statistic/*.hbase.dist



    
