import os
from class_xml.xml_writer import xml_writers,xml_node
from xml.dom.minidom import *

from log_analysis.getStructInfo import StructInfo
from log_analysis.getDetailInfo import DetailInfo
from log_analysis.getInstanceInfo import InstanceInfo
from config import local_config
from statistics_info.data_dist import server_idx_map
server_idx_map = local_config()['server_idx_map']
import re

class class_log:

    def __init__(self, log_file,xml_filename,logging):
        self.logging = logging
        self.xml_filename = xml_filename
        with open(log_file,'r') as fp:
            self.logfile = fp.read()
            self.part_impalad = (0, self.logfile.find('+----------+'))
            self.part_queryResult = (self.part_impalad[1], self.logfile.find('Query Runtime Profile', self.part_impalad[1] + 1))
            self.part_queryRuntime = (self.part_queryResult[1], self.logfile.find('----------------', self.part_queryResult[1] + 1))
            self.part_planFragment = (self.part_queryRuntime[1], self.logfile.find('----------------', self.part_queryRuntime[1] + 1))
            self.part_eachnodecosts = (self.part_planFragment[1], self.logfile.find('    Coordinator Fragment', self.part_planFragment[1] + 1))
            self.part_plannode = (self.part_eachnodecosts[1], self.logfile.find('\n    Fragment F',self.part_eachnodecosts[1]+1))
            self.part_instances = (self.part_plannode[1],len(self.logfile))
        #1.plan.xml
        self.obj_xml = xml_writers(xml_file = os.path.join(local_config()['install_dir'],'output_dir',(self.xml_filename)),query_name = xml_filename[0:-4])
        self.dom = self.obj_xml.create_dom()
        self.obj_node = xml_node(self.dom)
        
        #2.hbase.dist
        self.hbase_dist_file = os.path.join(local_config()['install_dir'],'output_dir',(self.xml_filename[0:-3]+'hbase.dist'))
        
#         with open('hehe.txt','w') as fp_t : print(self.logfile[self.part_instances[0]: self.part_instances[1]].strip('\n') ,file=fp_t)

    def getAttri(self):
        FragmentInfo_obj = StructInfo(self.logfile[self.part_planFragment[0] : self.part_planFragment[1]],self.logging)
        PlannodeInfo_obj = DetailInfo(self.logfile[self.part_plannode[0]:self.part_plannode[1]],self.obj_node,self.logging)
        self.infoTOwrite = FragmentInfo_obj.getAttri()
        self.infoTOwrite2 = PlannodeInfo_obj.getAttri()
        
        #.dist file
        InstanceInfo_obj = InstanceInfo(self.logfile[self.part_instances[0]:self.part_instances[1]].strip('\n'),self.obj_node,self.logging)
        self.hbasedist_info = InstanceInfo_obj.getAttri()
        
    def writeToXML(self):        
        for fragment in self.infoTOwrite:
            fg_node = self.obj_node.createNode('plan_fragment',fragment.get_attri())
            if fragment.dsm_info():
                dsm_node = self.obj_node.createNode('data_stream_sink',fragment.dsm_info().get_attri())
                fg_node.appendChild(dsm_node)
            for plannode in fragment.pl_nodes():
                plan_node = self.obj_node.createNode('plan_node',plannode.get_attri())
                if fragment.get_fid() in self.infoTOwrite2.keys() \
                    and plannode.get_nid() in self.infoTOwrite2[fragment.get_fid()].keys():
                    for item in self.infoTOwrite2[fragment.get_fid()][plannode.get_nid()]:
                        plan_node.appendChild(item)
#                 with open('hehe.txt','w') as fp_t:print >> fp_t, plannode.get_attri()
                fg_node.appendChild(plan_node)
            self.obj_xml.root_append(fg_node)
        self.obj_xml.save_xml()
        
    def writeToHBASEdist(self):
        with open(self.hbase_dist_file,'w') as fp:
            for fg in self.hbasedist_info.keys():
                for ins_dic in self.hbasedist_info[fg]:
                    for instance in ins_dic.keys():
                        for plnode in ins_dic[instance]:
                            if fg.getHasHBASENODE():
                                if re.search('HBASE_SCAN_NODE',plnode.getAttri()['n_key']):
                                    fp.write('%s,%s,%s,%s,%s,%s,%s,%s\n'%(fg.getAttri()['fid'],
                                                                         instance.getAttri()['host'],
                                                                         server_idx_map[instance.getAttri()['host']] if instance.getAttri()['host'] in server_idx_map.keys() else 0,
                                                                         plnode.getAttri()['nid'],
                                                                         plnode.getAttri()['RowsRead'],
                                                                         plnode.getAttri()['RowsReturned'],
                                                                         plnode.getAttri()['BytesRead'],
                                                                         plnode.getAttri()['TotalRawHBaseReadTime'],
                                                                     ))
                    
