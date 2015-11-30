# simulator_input
simulator's input
1.输入：
	①工程根目录创建 config.conf文件：
		##[Extract simulator's input from queries' profile]
		#switch_input=True
		#profile_path=W:\wangdewei\workspace\impala_pt\queries\logs\DATE2015-11-26_TIME10-05\fastest_logs\2.7
		#db_type=parquet
		#total_table_size_in_bytes=31522384490 #parquet_150
		## #total_table_size_in_bytes=1160538512799
		## #total_table_size_in_bytes=763065089970#parquet_3000
		## #total_table_size_in_bytes=11073840767#parquet_50
		## #total_table_size_in_bytes=18328355990#text_50
		##[Extract tuple descriptor from implalad log]
		#switch_tuple = False
		#impaladlog_path=W:\wangdewei\workspace\impala_pt\queries\logs\DATE2015-11-26_TIME10-05\fastest_logs\2.7
		##[RunBenchmark]
		#switch_runBenchmark=False
		#config_params_path=W:\junliu\Benchmark\Impala\Simulator (TEXT)\TEXT_3000_2.7G_10G_8DISK_6N_EST\params\q98.params
		#cof_dp_file=W:\junliu\Benchmark\Impala\Simulator (TEXT)\TEXT_3000_2.7G_10G_8DISK_6N_EST\params\cofs_dp.csv
		#block_list1=''
		#block_list2=''
	switch_*：switch开头的变量是开关，表示该段落是否被加载
    profile_path:profile文件存放的路径，既可为文件夹，也可以是单个profile文件
    impaladlog_path:impala守护进程的log路径，用于生成tuple_descriptor_*.xml
    db_type:text/parquet/parquet_snappy(以后可以修改为db_type and compresion_type)
    total_table_size_in_bytes: store_sales_*数据表大小，用字节表示，目前已经统计出来一部分，根据需要使用。
	RunBenchmark:是否运行模拟器
	config_params_path ：生成config.xml的参数文件的路径
	cof_dp_file：模拟器配置文件cofs_dp.csv
	block_list..：不需要运行的query的query名称
	②拷贝ImpalaSimulator.exe至main.py同级目录

2.输出：
	1.模拟器输入文件：output_dir 下获取执行计划*.xml/*.dist /*.statistic/*.hbase.dist
	2.模拟器运行时log:./benchmark



    