import re
from config import local_config
class Plannode:
#     def __init__(self,pl_i,plnode,logging,LR): 
#         self.attri_dic = {}
#         self.logging = logging
#         self.attri_dic['lr'] = LR
#         
#         if pl_i == 0 :
#             self.attri_dic['is_plan_root'] = 'true'
#         else:
#             self.attri_dic['is_plan_root'] = 'false'
#             
#         plnode_lines = plnode.split('\n')
#         for line in plnode_lines:
#             line = line.strip()
#             line = line.strip('|')
#             line = line.strip('--')
#             line = line.strip()
#             if ':' in line:
#                 if line.count(':') > 1:
#                     continue
#                 line_key,line_val = line.split(':')
#                 if line_key.isdigit():
#                     distribution_type = {}
#                     if ' [' in line_val:
#                         dispname ,dist_type = line_val.split(' [')
#                         dist_type = dist_type.strip(']')
#                         if ', ' in dist_type :
#                             join_type,dist_type = dist_type.split(', ')
#                             self.attri_dic['join_type'] = join_type
#                         self.ana_Distributiontype(dist_type)
#                     else:
#                         dispname = line_val
#                     self.attri_dic['display_name'] = dispname
#                   
#                     self.attri_dic['nid'] = str(int(line_key))
#                 if 'limit' in line_key \
#                     or 'LIMIT' in line_val:
#                     limit_num = line[line.lower().find('limit')+5:]
#                     for char_sp in local_config()['char_list']:
#                         limit_num = limit_num.strip(char_sp)
#                     self.attri_dic['limit'] = str(int(limit_num))
#             else:
#                 if 'tuple-ids' in line \
#                     and 'row-size' in line \
#                     and 'cardinality' in line:
#                     tuple_ids,row_size,cardinality, = line.split(' ')
#                     self.attri_dic['tuple-ids'] = tuple_ids.split('=')[1]
#                     self.attri_dic['row-size'] = str(int(row_size.split('=')[1][:-1]))
                    
    def __init__(self,pl_i,plnode,logging,LR):
        self.attri_dic = {}
        self.logging = logging
        self.attri_dic['lr'] = LR
        
        if pl_i == 0 :
            self.attri_dic['is_plan_root'] = 'true'
        else:
            self.attri_dic['is_plan_root'] = 'false'
        for line in plnode.split('\n'):
            line = line.strip()
            line = line.strip('|')
            line = line.strip('--')
            line = line.strip()
            if re.search('\d+:',line) and 'nid' not in self.attri_dic.keys():
                line_key,line_val = line.split(':')
                self.attri_dic['nid'] = str(int(line_key))
                node_type,node_type_attri = (' [' in line_val) and line_val.split(' [') or [line_val,None]
                self.attri_dic['display_name'] = node_type
                if node_type_attri:
                    node_type_attri = node_type_attri.strip(']')
                    if re.match('HASH JOIN',node_type):
                        join_type,dist_type = node_type_attri.split(', ')
                        self.attri_dic['join_type'] = join_type
                        self.ana_Distributiontype(dist_type)
                    elif re.match('SCAN HDFS', node_type):
                        scan_table,dist_type = node_type_attri.split(', ')
                        scan_db,scan_tbl = scan_table.split('.')
                        self.attri_dic['database_name'] = scan_db
                        self.attri_dic['table'] = scan_tbl
                        self.ana_Distributiontype(dist_type)
                    elif re.match('TOP-N', node_type):
                        self.attri_dic['limit'] = str(int(node_type_attri.split('=')[1]))
                    else:    
                        self.ana_Distributiontype(node_type_attri)
            #NODE:MERGE-EXCHANGE maybe has limit attribute       
            m = re.match('limit:\s\d+',line)
            if m:
                self.attri_dic['limit'] = str(int(line.split(': ')[1]))
            if self.attri_dic['display_name'] == 'SCAN HDFS':
                m = re.match('partitions=\d+/\d+',line)
                if m:
                    self.attri_dic['partitions'] = str(m.group(0).split('=')[1]) 
                    
            if 'tuple-ids' in line \
                    and 'row-size' in line \
                    and 'cardinality' in line:
                    tuple_ids,row_size,cardinality, = line.split(' ')
                    self.attri_dic['tuple-ids'] = tuple_ids.split('=')[1]
                    self.attri_dic['row-size'] = str(int(row_size.split('=')[1][:-1]))
            
    def ana_Distributiontype(self,dist_type):
        if re.compile('HASH\((\w+.)+\)').match(dist_type):
            dist_mode="PARTITIONED"
            partition_type="HASH"
            self.attri_dic['dist_mode'] = dist_mode
            self.attri_dic['partition_type'] = partition_type
        elif re.compile('\w*UNPARTITIONED\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "UNPARTITIONED"
            self.attri_dic['dist_mode'] = dist_mode
            self.attri_dic['partition_type'] = partition_type
        elif re.compile('\w*RANDOM\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANDOM"
            self.attri_dic['dist_mode'] = dist_mode
            self.attri_dic['partition_type'] = partition_type
        elif re.compile('\w*RANGE\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANGE"
            self.attri_dic['dist_mode'] = dist_mode
            self.attri_dic['partition_type'] = partition_type
        elif re.compile('\w*BROADCAST\w*').match(dist_type):
            dist_mode = "BROADCAST"
            partition_type = None
            self.attri_dic['dist_mode'] = dist_mode
        else:
            self.logging.error('no dist type:%s'%(dist_type))
    
    def get_nid(self):
        return self.attri_dic['nid']
    
    def set_child(self,child_nid):
        if 'children' not in self.attri_dic.keys():
            self.attri_dic['children'] = str(child_nid)
        else:
            self.attri_dic['children'] += ',' +str(child_nid)
            
    def get_LR(self):
        return self.attri_dic['lr']
    
    def get_attri(self):
        if 'children' not in self.attri_dic.keys():
            self.attri_dic['children'] = ""
        des_dic = {
                'nid':self.attri_dic['nid'],
                'is_plan_root':self.attri_dic['is_plan_root'],
                'display_name':self.attri_dic['display_name'],
                'children': self.attri_dic['children'],
                'tuple-ids': self.attri_dic['tuple-ids'],
                'row-size': self.attri_dic['row-size'],
                }
        if 'limit'in self.attri_dic.keys():
            des_dic['limit'] = self.attri_dic['limit']
        if 'dist_mode' in self.attri_dic.keys():
            des_dic['dist_mode'] = self.attri_dic['dist_mode']
        if 'partition_type' in self.attri_dic.keys():
            des_dic['partition_type'] = self.attri_dic['partition_type']
        if 'join_type' in self.attri_dic.keys():
            des_dic['join_type'] = self.attri_dic['join_type']
        if 'database_name' in self.attri_dic.keys() \
            and 'table' in self.attri_dic.keys():
            des_dic['database_name'] = self.attri_dic['database_name']
            des_dic['table'] = self.attri_dic['table']
        if 'partitions' in self.attri_dic.keys():
            des_dic['partitions'] = self.attri_dic['partitions']
        return des_dic
        