from config import local_config
import re

class Fragment:
    def __init__(self,attri_dic):
        self.attri_dic = attri_dic
    def get_attri(self):
        return {
                    'fid':self.attri_dic['fid'],
                    'is_root':self.attri_dic['is_root']
                }
    def dsm_info(self):
        if 'data_stream_sink' in self.attri_dic.keys():
            return self.attri_dic['data_stream_sink']
        else:
            return None    
          
    def pl_nodes(self):
        return self.attri_dic['pl_nodes']

    def get_fid(self):
        return self.attri_dic['fid']

       