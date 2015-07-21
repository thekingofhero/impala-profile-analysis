from config import local_config
class Fragment:
    def __init__(self,attri_dic):
        self.attri_dic = attri_dic
        for key in self.attri_dic.keys():
            print key,self.attri_dic[key]
            
class FragmentInfo:
    def __init__(self, logfile):
        self.logfile = logfile
        
    def getFragments(self):
        fragments = self.logfile.split('\n\n')[1:]
        return fragments
#         for fragment in fragments:   
    def getAttri(self):
        
        fragments = self.getFragments()
        for fg in fragments:
            fg_attri_dic = {}
            fg_lines = fg.split('\n')
            for line in fg_lines:
                line = line.strip()
                if ':' in line:
                    line_key,line_val = line.split(':')
                    if 'PLAN FRAGMENT' in line_val:
                        fg_attri_dic['fid'] = line_key[1:] 
                    if line_key.isdigit():
                        fg_attri_dic['display_name'] = line_val
                        fg_attri_dic['nid'] = int(line_key)
                    if 'limit' in line_key \
                        or 'LIMIT' in line_val:
                        limit_num = line[line.lower().find('limit')+5:]
                        for char_sp in local_config()['char_list']:
                            limit_num = limit_num.strip(char_sp)
                        fg_attri_dic['limit'] = int(limit_num)
                else:
                    if 'tuple-ids' in line \
                        and 'row-size' in line \
                        and 'cardinality' in line:
                        tuple_ids,row_size,cardinality, = line.split(' ')
                        fg_attri_dic['tuple_ids'] = int(tuple_ids.split('=')[1])
                        fg_attri_dic['row_size'] = int(row_size.split('=')[1][:-1])
            fr = Fragment(fg_attri_dic)
            break
        
 
            
        
    