#-*- coding:utf-8 -*-
import re
class Instance:
    def __init__(self,instance,logging):
        self.instance = instance
        self.logging = logging
        self.attri = {}
        
        lines = self.instance.split('\n')
        for line in lines:
            line = line.strip()
            if "(host=" in line \
                and ":22000):(" in line:
                lft_idx = line.find("(host=")
                rgh_idx = line.find(":22000):(")
                host_name = line[lft_idx+6:rgh_idx]
                self.attri['host'] = host_name        
    def getAttri(self):
        
                
        return self.attri        
