from log_analysis.class_log import class_log
import logging
import os
from config import local_config
from statistics.gather_stats import gather_stats
from statistics.data_dist import collect_data_dist_info
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
                if file.endswith('.log'):
                    query_name = file.split('.')[0]
                    if query_name not in task_dic.keys():
                        task_dic[query_name] = [os.path.join(root,file),query_name+'.sql.xml']                    
    elif os.path.isfile(local_config()['profile_path']):
        file = os.path.split(local_config()['profile_path'])[-1]
        query_name = file.split('.')[0]
        if query_name not in task_dic.keys():
            task_dic[query_name] = [local_config()['profile_path'],query_name+'.sql.xml']
    return task_dic

if __name__ == '__main__':
    if 'Linux' in local_config()['current_sys']:
        os.system('rm -f ./output_dir/* ')
    elif 'Windows' in local_config()['current_sys']:
        os.system('del .\output_dir\*  /q ')
    task_dic = get_task_dic()
    for task in task_dic.keys():
        #1.xml
        obj = class_log(task_dic[task][0],task_dic[task][1],logging)
        obj.getAttri()
        obj.writeToXML()
        obj.writeToHBASEdist()
        #2.dist
        collect_data_dist_info(task_dic[task][0],os.path.join(local_config()['install_dir'],task_dic[task][1][0:-3] + 'dist'))
        #2.dist and statistics
        #gather_stats(os.path.join(task_dic[task][0],task_dic[task][1],logging),os.path.join(local_config()['install_dir'],'output_dir'))