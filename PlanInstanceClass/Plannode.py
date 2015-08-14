#-*- coding:utf-8 -*-
import re
from scripts.time_trans import get_time_by_second
class Plannode:
    def __init__(self,n_key,plnode,obj_node,logging):
        self.plnode = plnode
        self.logging = logging
        self.attri = {}
        self.obj_node = obj_node
        self.node_case =[
                            'RowsRead',
                            'RowsReturned',
                            'BytesRead',
                            'TotalRawHBaseReadTime'
                          ]
        
        
        lines = self.plnode.split('\n')
        self.attri['n_key'] = n_key
        for line in lines:
            nid = re.search('\(id=\d+\)',line) 
            if nid is not None and 'nid' not in self.attri.keys():
                self.attri['nid'] = str(int(nid.group(0).split('=')[1][:-1]))
            for item in self.node_case:
                #两个IF的顺序不能反，先匹配带()的，再匹配无括号的。原因是re.match并不是完全匹配，只是从字符串的开头匹配，然后返回第一次出现
                ## - RowsReturned: 20.20K (20201)
                rows = re.match('\s+- %s: \d+(\.\d+)?\s*\w* \(\d+\)'%(item),line)
#                 print '\s+- %s: \d+(\.\d+)?\w \(\d+\)'%(item)
                if rows:
                    temp = str(int(rows.group(0).split('(')[-1].strip(')')) )
                    self.attri[item] = temp
                ## - RowsReturned: 144  
                else:
                    rows = re.match('\s+- %s: \d+'%(item),line)
                    
                    if rows:
                        temp = str(int(line.split(':')[1]) )
                        self.attri[item] = temp
                #TotalRawHBaseReadTime(*)
                rows = re.search('\s+- %s\(\*\):'%(item),line)
                if rows:
                    time = get_time_by_second(line.split(':')[1])
                    self.attri[item] = time
                     
    def getAttri(self):
        
                
        return self.attri        
