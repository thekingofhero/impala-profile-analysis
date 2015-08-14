from config import local_config
from PlanStructClass.Fragment import Fragment
from PlanStructClass.Plannode import Plannode
from PlanStructClass.DSM_sink import DSM_sink
import re


class StructInfo:
    def __init__(self, logfile,logging):
        self.logging = logging
        self.logfile = logfile
        
    def getFragments(self):
        fragments = self.logfile.split('\n\n')[1:]
        return fragments
#         for fragment in fragments:   
    def getPlannodes(self,plnodes):
        return plnodes.split('\n  |\n')
     
 
    def getAttri(self):
        fragments = self.getFragments()
        frInfo_list = []
        for fg_i,fg in enumerate(fragments):
            fg_attri_dic = {}
            #PLAN FRAGMENT
            if fg_i == 0:
                fg_attri_dic['is_root'] = 'true'
            else:
                fg_attri_dic['is_root'] = 'false'
                
            line = fg[0:fg.find('\n')]
            line = line.strip()
            if ':' in line:
                line_key,line_val = line.split(':')
                if 'PLAN FRAGMENT' in line_val:
                    fg_attri_dic['fid'] = str(int(line_key[1:]))
                fg = fg[fg.find('\n')+1:]
            #DATASTREAM SINK
            line = fg[0:fg.find('\n')]
            line = line.strip()
            if 'DATASTREAM SINK' in line:
                fg_attri_dic['data_stream_sink'] = DSM_sink(line,self.logging)
                fg = fg[fg.find('\n')+1:]
                
            plnodes = self.getPlannodes(fg)
            fg_attri_dic['pl_nodes'] = []
            plnode_right_child = [0,'']
            for pl_i,plnode in enumerate(plnodes):
                #right_child_plannode
                if '|--' in plnode :
                    plnode_right_child = [pl_i,plnode]
                    continue
                plan_node = Plannode(pl_i,plnode,self.logging,'L')
                fg_attri_dic['pl_nodes'].append(plan_node)
                if pl_i > 0:
                    dad_offset = 0
                    if fg_attri_dic['pl_nodes'][-2].get_LR() == 'R':
                        dad_offset = 1
                    fg_attri_dic['pl_nodes'][-2 - dad_offset].set_child(plan_node.get_nid())
                    if plnode_right_child != [0,''] :
                        plan_node = Plannode(plnode_right_child[0],plnode_right_child[1],self.logging,'R')
                        fg_attri_dic['pl_nodes'].append(plan_node)
                        fg_attri_dic['pl_nodes'][-3 - dad_offset].set_child(plan_node.get_nid())
                        plnode_right_child = [0,'']
            fr = Fragment(fg_attri_dic)
            frInfo_list.append(fr)
        return frInfo_list

            
        
    