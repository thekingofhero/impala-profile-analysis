# simulator_input
simulator's input
1.update config.py
    profile_path:profile文件存放的路径，既可为文件夹，也可以是单个profile文件
    db_type:text/parquet/parquet_snappy(以后可以修改为db_type and compresion_type)
    db_size:50/3000...
    #no change下的变量一般不用修改
    total_table_size_in_bytes_dic 目前只统计了db_size = 50 和db_size=3000的数据库store_sales表的文件大小

2.python main.py
    under python2.7
    已更新至python3

3.output_dir 下获取执行计划＊.xml


PS:目前进度：
    2015-08-06：生成包含data-percentage的执行计划文件（*.xml）