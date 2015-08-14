import re
class Fragment:
    def __init__(self,fg_text,logging):
        self.fg_text = fg_text
        self.logging = logging
        self.attri_dic = {}
        
        lines = self.fg_text.split('\n')
        for line in lines:
            line = line.strip()
            fid = re.match(r'\d+:',line)
            if fid:
                self.attri_dic['fid'] = int(line.split(':')[0])
        self.attri_dic['has_hdfsscannode'] = False
        self.attri_dic['has_hbasescannode'] = False
        
    def getAttri(self):
        return self.attri_dic

    def setHasHDFSNODE(self):
        self.attri_dic['has_hdfsscannode'] = True
    
    def getHasHDFSNODE(self):
        return self.attri_dic['has_hdfsscannode']
    
    def setHasHBASENODE(self):
        self.attri_dic['has_hbasescannode'] = True
    
    def getHasHBASENODE(self):
        return  self.attri_dic['has_hbasescannode']