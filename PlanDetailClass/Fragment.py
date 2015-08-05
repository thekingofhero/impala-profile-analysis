import re
class Fragment:
    def __init__(self,fg_text,logging):
        self.fg_text = fg_text
        self.logging = logging
        self.attri_dic = {}
        
        lines = self.fg_text.split('\n')
        for line in lines:
            r = '\d+:\('
            m = re.search(r,line)
            
            if m and 'fid' not in self.attri_dic.keys():
                
                self.attri_dic['fid'] = str(int(m.group(0)[0:-2]))
            
            r = 'num instances'
            m = re.search(r,line)
            if m and 'num_ins' not in self.attri_dic.keys():
                self.attri_dic['num_ins'] = int(line.split(':')[-1])
        self.attri_dic['plan_nodes'] = {}
        
    def getAttri(self):
        
        return self.attri_dic

    def getNumIns(self):
        if 'num_ins' in self.attri_dic.keys():
            return self.attri_dic['num_ins']
        else:
            return 1