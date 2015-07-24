from config import local_config
from PlanClass import Fragment
from PlanClass import Plannode
import re
import logging
logging.basicConfig(
                    level=logging.DEBUG,
                    format="%(asctime)s %(filename)s %(levelname)s %(message)s",
                    datefmt="%a,%d %b %Y %H:%M:%S",
                    filename='mylog.log',
                    filemode='w')


class FragmentInfo:
    def __init__(self, logfile):
        self.logfile = logfile
        
    def getFragments(self):
        fragments = self.logfile.split('\n\n')[1:]
        return fragments
#         for fragment in fragments:   
    def getPlannodes(self,plnodes):
        return plnodes.split('\n  |\n')
    
    def ana_Distributiontype(self,dist_type):
        if re.compile('HASH\((\w+.)+\)').match(dist_type):
            dist_mode="PARTITIONED"
            partition_type="HASH"
            distribution_type = 'dist_mode="%s",partition_type="%s"'%(dist_mode,partition_type)
        elif re.compile('\w*UNPARTITIONED\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "UNPARTITIONED"
            distribution_type = 'dist_mode="%s",partition_type="%s"'%(dist_mode,partition_type)
        elif re.compile('\w*RANDOM\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANDOM"
            distribution_type = 'dist_mode="%s",partition_type="%s"'%(dist_mode,partition_type)
        elif re.compile('\w*RANGE\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANGE"
            distribution_type = 'dist_mode="%s",partition_type="%s"'%(dist_mode,partition_type)
        elif re.compile('\w*BROADCAST\w*').match(dist_type):
            dist_mode = "BROADCAST"
            partition_type = None
            distribution_type = 'dist_mode="%s"'%(dist_mode)
        else:
            logging.error('no dist type:%s'%(dist_type))
        return distribution_type
    
    def ana_datastreamsink(self,dsms):
        dsms = dsms[dsms.find('[') + 1 : dsms.find(']')]
        ex_frgid,ex_nodeid,dist_type = dsms.split(', ')
        distribution_type = self.ana_Distributiontype(dist_type)
        return ['fid="%d"'%(int(ex_frgid.split('=')[1][1:])), \
                'nid="%d"'%(int(ex_nodeid.split('=')[1])), \
                distribution_type
                ]
            
    def getAttri(self):
        fragments = self.getFragments()
        frInfo_list = []
        for fg_i,fg in enumerate(fragments):
            fg_attri_dic = {}
            #PLAN FRAGMENT
            if fg_i == 0:
                fg_attri_dic['is_root'] = True
            else:
                fg_attri_dic['is_root'] = False
                
            line = fg[0:fg.find('\n')]
            line = line.strip()
            if ':' in line:
                line_key,line_val = line.split(':')
                if 'PLAN FRAGMENT' in line_val:
                    fg_attri_dic['fid'] = line_key[1:]
                fg = fg[fg.find('\n')+1:]
            #DATASTREAM SINK
            line = fg[0:fg.find('\n')]
            line = line.strip()
            if 'DATASTREAM SINK' in line:
                fg_attri_dic['data_stream_sink'] = self.ana_datastreamsink(line)
                fg = fg[fg.find('\n')+1:]
                
            plnodes = self.getPlannodes(fg)
            fg_attri_dic['pl_nodes'] = []
            for pl_i,plnode in enumerate(plnodes):
                plnode_attri_dic = {}
                if pl_i == 0 :
                    plnode_attri_dic['is_plan_root'] = True
                else:
                    plnode_attri_dic['is_plan_root'] = False
                    
                plnode_lines = plnode.split('\n')
                for line in plnode_lines:
                    line = line.strip()
                    line = line.strip('|')
                    line = line.strip()
                    if ':' in line:
                        if line.count(':') > 1:
                            continue
                        line_key,line_val = line.split(':')
                        if line_key.isdigit():
                            plnode_attri_dic['display_name'] = line_val
                            plnode_attri_dic['nid'] = int(line_key)
                        if 'limit' in line_key \
                            or 'LIMIT' in line_val:
                            limit_num = line[line.lower().find('limit')+5:]
                            for char_sp in local_config()['char_list']:
                                limit_num = limit_num.strip(char_sp)
                            plnode_attri_dic['limit'] = int(limit_num)
                    else:
                        if 'tuple-ids' in line \
                            and 'row-size' in line \
                            and 'cardinality' in line:
                            tuple_ids,row_size,cardinality, = line.split(' ')
                            plnode_attri_dic['tuple_ids'] = tuple_ids.split('=')[1]
                            plnode_attri_dic['row_size'] = int(row_size.split('=')[1][:-1])
                plnode = Plannode(plnode_attri_dic)
                fg_attri_dic['pl_nodes'].append(plnode)
            fr = Fragment(fg_attri_dic)
            frInfo_list.append(fr)
            if fg_i == 3:
                break
        return frInfo_list
        
 
            
        
    