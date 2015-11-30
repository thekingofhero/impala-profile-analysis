from log_analysis.class_log import class_log
import logging
import os
from config import local_config
from gather_stats import gather_stats
from scripts.slots_info import gather_slots_info
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
                if file.endswith('.hehe024.impaladlog'):
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
    #Simulator inputs 
    if local_config()['sw_input'] is True:
        task_dic = get_task_dic()
        for task in task_dic.keys():
            print(task_dic[task][0])
            #1.xml
            obj = class_log(task_dic[task][0],task_dic[task][1],logging)
            obj.getAttri()
            obj.writeToXML()
            #gather statistic and dist file 
            gather_stats(task_dic[task][0],"output_dir")
            #hbase dist
            obj.writeToHBASEdist()
    
    #tuple_descriptor.xml
    if local_config()['sw_tuple'] is True:
        task_dic = get_tuple_task_dic()
        for task in task_dic.keys():
            print(task_dic[task][0])
            #tuple.xml
            gather_slots_info(task_dic[task][0],os.path.join(os.path.join(local_config()['install_dir'],"output_dir",task_dic[task][1])))
        
    #Run Benchmark   
    if local_config()['sw_runBenchmark'] is True:
        from benchmark.benchmark import runBenchMark
        runBenchMark(local_config()['config_params_path'],
                     local_config()['cof_dp_file'],
                     local_config()['block_list'])