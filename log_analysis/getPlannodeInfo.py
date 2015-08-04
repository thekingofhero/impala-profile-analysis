from config import local_config
import re
from compiler.ast import Node, nodes
from PlanDetailClass.Plannode import Plannode
from PlanDetailClass.Fragment import Fragment

class PlannodeInfo:
    def __init__(self,logfile,obj_node,logging):
        self.logfile = logfile
        self.logging = logging
        self.obj_node = obj_node
        self.attri = {}
        
    def getFragments(self,):
        ff = re.split('\s+Coordinator Fragment F|\s+Averaged Fragment F', self.logfile)
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
    def getNodes(self,fragment):
        r = "\n\s+EXCHANGE_NODE|SORT_NODE|AGGREGATION_NODE|HASH_JOIN_NODE|HDFS_SCAN_NODE"
        r_keys = re.findall(r,fragment)
        nn = re.split(r, fragment)
        fragment_info = nn[0]
        nodes = nn[1:]
        if len(nodes) <> len(r_keys):
            self.logging.error( 'node,keys unequal')
        return fragment_info,map(None,r_keys,nodes)
    
    def getAttri(self,):
#         fp = open('hehe','w')
        fragments = self.getFragments()
        
        
        for fragment in fragments:
            fragment_info,nodes = self.getNodes(fragment)
            fg = Fragment(fragment_info,self.logging)
            fg_attri = fg.getAttri()
            
            for n_key,node in nodes:
                plnode = Plannode(node,self.obj_node,fg.getNumIns(),self.logging)
                plnode_attri = plnode.getAttri()
                fg_attri['plan_nodes'][plnode_attri['nid']] = plnode_attri['items']
#            print len(self.attri.keys())
            self.attri[fg_attri['fid']] = fg_attri['plan_nodes']
           
        return self.attri    
#         print >> fp,'lala:',self.attri
#         fp.close()
            