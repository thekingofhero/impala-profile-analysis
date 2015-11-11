from log_analysis.class_log import class_log
import logging
import os
from config import local_config
from config_gen.config_gen import generate_config
from statistics_info.data_dist import collect_data_dist_info
from statistics_info.hj_metrics import collect_statistics as hj_cs
from statistics_info.aggr_metrics import collect_statistics as aggr_cs
from statistics_info.exchange import collect_statistics as ex_cs
from statistics_info.data_stream_sender import collect_statistics as dss_cs
from statistics_info.scan_stats import collect_scanner_stats_info as scan_cs
from config_gen.slots_info import gather_slots_info
logging.basicConfig(
                    level=logging.DEBUG,
                    format="%(asctime)s %(filename)s %(levelname)s %(message)s",
                    datefmt="%a,%d %b %Y %H:%M:%S",
                    filename='mylog.log',
                    filemode='w')
def get_task_dic():
    task_dic = {}
    if os.path.isdir(local_config()['profile_path']):
        for root,dirs,files in os.walk(local_config()['profile_path']):
            for file in files:
                if file.endswith('.sql.log'):
                    query_name = file.split('.')[0]
                    if query_name not in task_dic.keys():
                        task_dic[query_name] = [os.path.join(root,file),query_name+'.sql.xml']                    
    elif os.path.isfile(local_config()['profile_path']):
        file = os.path.split(local_config()['profile_path'])[-1]
        query_name = file.split('.')[0]
        if query_name not in task_dic.keys():
            task_dic[query_name] = [local_config()['profile_path'],query_name+'.sql.xml']
    return task_dic

def get_tuple_task_dic():
    task_dic = {}
    if os.path.isdir(local_config()['impaladlog_path']):
        for root,dirs,files in os.walk(local_config()['impaladlog_path']):
            for file in files:
                if file.endswith('.tracing024.impaladlog'):
                    query_name = file.split('.')[0]
                    if query_name not in task_dic.keys():
                        task_dic[query_name] = [os.path.join(root,file),'tuple_descriptor_'+query_name+'.xml']                    
    elif os.path.isfile(local_config()['impaladlog_path']):
        file = os.path.split(local_config()['impaladlog_path'])[-1]
        query_name = file.split('.')[0]
        if query_name not in task_dic.keys():
            task_dic[query_name] = [local_config()['impaladlog_path'],'tuple_descriptor_'+query_name+'.xml']
    return task_dic

if __name__ == '__main__':
    if 'Linux' in local_config()['current_sys']:
        os.system('rm -f ./output_dir/* ')
    elif 'Windows' in local_config()['current_sys']:
        os.system('del .\output_dir\*  /q ')
    task_dic = get_task_dic()
    for task in task_dic.keys():
        print(task_dic[task][0])
        #1.xml
        obj = class_log(task_dic[task][0],task_dic[task][1],logging)
        obj.getAttri()
        obj.writeToXML()
        #hbase dist
        obj.writeToHBASEdist()
        #2.dist
        collect_data_dist_info(task_dic[task][0],os.path.join(local_config()['install_dir'],"output_dir",task_dic[task][1][0:-3] + 'dist'))
        #3.statistics
        statistic_file = os.path.join(local_config()['install_dir'],"output_dir",task_dic[task][1][0:-3] + 'statistic')
        with open(statistic_file,'w') as fp:
            hj_cs(task_dic[task][0],statistic_file + '.csv',file_p = fp)
            aggr_cs(task_dic[task][0], statistic_file + '.csv',file_p = fp)
            ex_cs(task_dic[task][0], statistic_file + '.csv',file_p = fp)
            dss_cs(task_dic[task][0], statistic_file + '.csv',file_p = fp)
            scan_cs(task_dic[task][0], statistic_file + '.csv',file_p = fp)
       
        #4.config.XML
        generate_config(os.path.join(local_config()['install_dir'],'config_gen','config.template'),\
                        os.path.join(local_config()['install_dir'],'config_gen','config.params'),\
                        os.path.join(local_config()['install_dir'],"output_dir"))
    task_dic = get_tuple_task_dic()
    for task in task_dic.keys():
        print(task_dic[task][0])
        #4.tuple.xml
        gather_slots_info(task_dic[task][0],os.path.join(os.path.join(local_config()['install_dir'],"output_dir",task_dic[task][1])))