class Fragment:
    def __init__(self,attri_dic):
        self.attri_dic = attri_dic
        
    def fid(self):
        return self.attri_dic['fid']
    
    def dsm_info(self):
        if 'DATASTREAM SINK' in self.attri_dic.keys():
            return self.attri_dic['DATASTREAM SINK']
        else:
            return None
         
    def pl_nodes(self):
        return self.attri_dic['pl_nodes']
    
    def is_root(self):
        return self.attri_dic['is_root']

class DSM_sink:
    def __init__(self,attri_dic):
        self.attri_dic = attri_dic 
    def fid(self):
        return self.attri_dic =    

class Plannode:
    def __init__(self,attri_dic): 
        self.attri_dic = attri_dic
                   