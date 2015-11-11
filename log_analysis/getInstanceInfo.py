from config import local_config
import re
from PlanInstanceClass.Plannode import Plannode
from PlanInstanceClass.Fragment import Fragment
from PlanInstanceClass.Instance import Instance
class InstanceInfo:
    def __init__(self,logfile,obj_node,logging):
        self.logfile = logfile
        self.logging = logging
        self.obj_node = obj_node
        self.attri = {}
        
    def getFragments(self,):
        ff = re.split('\s+Fragment F', self.logfile)
        fragments = []
        for fragment in ff:
            if fragment is None or len(fragment) == 0:
                continue
            fragments.append(fragment)
        return fragments
    
#     def getNodes(self,fragment):
#         r = "\n      \w+"
#         r_keys = re.findall(r, fragment)
#         nn = re.split(r,fragment)
#         nodes = []
#         
#         for node in nn:
#             if node is None or len(node) == 0:
#                 continue
#             nodes.append(node)
#         des = map(None ,r_keys,nodes)
#         return des
    def getInstances(self,fragment):
        r = "Instance"
        ii = re.split(r,fragment)
        return ii
    
    def getNodes(self,instance):
        r = "\n\s+EXCHANGE_NODE|SORT_NODE|AGGREGATION_NODE|HASH_JOIN_NODE|HDFS_SCAN_NODE|HBASE_SCAN_NODE"
        r_keys = re.findall(r,instance)
        nn = re.split(r, instance)
        instance_info = nn[0]
        nodes = nn[1:]
        if len(nodes) != len(r_keys):
            self.logging.error( 'node,keys unequal')
        
        return instance_info,map(lambda x,y:(x,y),r_keys,nodes)
    
    def getAttri(self,):
        fragments = self.getFragments()
        for fragment in fragments:
            fg_info = fragment[0:fragment.find('\n')]
            fg = Fragment(fg_info,self.logging)
            self.attri[fg] = []    
            instances = self.getInstances(fragment[fragment.find('\n')+1:])
            for instance in instances:
                instance_info,nodes = self.getNodes(instance)
                Instance_obj = Instance(instance_info,self.logging)
                ins_dic = {}
                ins_dic[Instance_obj] = []
                for n_key,node in nodes:
                    if re.search('HDFS_SCAN_NODE',n_key) :
                        fg.setHasHDFSNODE()
                    elif re.search('HBASE_SCAN_NODE',n_key):
                        fg.setHasHBASENODE()
                    plnode = Plannode(n_key,node,self.obj_node,self.logging)
                    ins_dic[Instance_obj].append(plnode)
                self.attri[fg].append(ins_dic)
#         with open('hehe.txt','w') as fp: print ('lala:',self.attri,file = fp)
            
                
        return self.attri    

            