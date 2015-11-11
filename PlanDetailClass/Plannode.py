#-*- coding:utf-8 -*-
import re
class Plannode:
    def __init__(self,plnode,obj_node,num_ins,logging):
        self.plnode = plnode
        self.logging = logging
        self.attri = {}
        self.obj_node = obj_node
        self.num_ins = num_ins
        self.node_case = {
                            'RowsReturned':'rows_returned',
                            'ProbeRows':'probe_rows',
                            'BuildRows':'build_rows',
                            'RowsRead': 'rows_read'
                          }
        
        
        lines = self.plnode.split('\n')
        self.attri['items'] = []
        for line in lines:
            nid = re.search('\(id=\d+\)',line) 
            if nid is not None and 'nid' not in self.attri.keys():
                self.attri['nid'] = str(int(nid.group(0).split('=')[1][:-1]))
            
            for item in self.node_case.keys():
                #两个IF的顺序不能反，先匹配带()的，再匹配无括号的。原因是re.match并不是完全匹配，只是从字符串的开头匹配，然后返回第一次出现
                ## - RowsReturned: 20.20K (20201)
                rows = re.match('\s+- %s: \d+(\.\d+)?\w* \(\d+\)'%(item),line)
#                 print '\s+- %s: \d+(\.\d+)?\w \(\d+\)'%(item)
                if rows:
                    temp = str(int(rows.group(0).split('(')[-1].strip(')')) * self.num_ins)
                    child_node = self.obj_node.createNode(self.node_case[item],text=temp)
                    self.attri['items'].append(child_node)
                ## - RowsReturned: 144  
                else:
                    rows = re.match('\s+- %s: \d+'%(item),line)
                    if rows:
                        temp = str(int(line.split(':')[1]) * self.num_ins)
                        child_node = self.obj_node.createNode(self.node_case[item],text=temp)
                        self.attri['items'].append(child_node)

    def getAttri(self):
        
                
        return self.attri        
