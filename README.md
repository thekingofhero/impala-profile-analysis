# simulator_input
simulator's input
1.输入：
	工程根目录创建 config.conf文件：
		# config.conf's content:
		# profile_path=\\10.239.44.78\home\wangdewei\workspace\impala_pt\queries\logs\DATE2015-08-28_TIME16-32\fastest_logs\2.7
		# impaladlog_path=\\10.239.44.78\home\wangdewei\workspace\impala_pt\queries\logs\DATE2015-08-28_TIME16-32\fastest_logs\2.7
		# db_type=text
		# total_table_size_in_bytes=1160538512799#text_3000
		# #total_table_size_in_bytes=763065089970#parquet_3000
		# #total_table_size_in_bytes=11073840767#parquet_50
		# #total_table_size_in_bytes=18328355990#text_50
    profile_path:profile文件存放的路径，既可为文件夹，也可以是单个profile文件
    impaladlog_path:impala守护进程的log路径，用于生成tuple_descriptor_*.xml
    db_type:text/parquet/parquet_snappy(以后可以修改为db_type and compresion_type)
    #no change下的变量一般不用修改
    total_table_size_in_bytes_dic store_sales_*数据表大小，用字节表示，目前已经统计出来一部分，根据需要使用。


2.输出：
	output_dir 下获取执行计划＊.xml等


PS:目前进度：
    2015-08-06：生成包含data-percentage的执行计划文件（*.xml）
    2015-08-21:生成执行计划（*.xml）、dist、statistic；config.xml已添加，正确性待检验。
    2015-08-28：add tuple_descriptor.xml

    