from config import local_config
import re

#data_stream_sink
class DSM_sink:
    def __init__(self,dsms,logging):
        self.logging = logging
        dsms = dsms[dsms.find('[') + 1 : dsms.find(']')]
        ex_frgid,ex_nodeid,dist_type = dsms.split(', ')
        distribution_type = self.ana_Distributiontype(dist_type)
        self.attri_dic = {}
        self.attri_dic['fid'] = str(int(ex_frgid.split('=')[1][1:]))
        self.attri_dic['nid'] = str(int(ex_nodeid.split('=')[1]))
        for key in distribution_type.keys():
            if key not in self.attri_dic.keys():
                self.attri_dic[key] = distribution_type[key]
    def ana_Distributiontype(self,dist_type):
        distribution_type = {}
        if re.compile('HASH').match(dist_type):
            dist_mode="PARTITIONED"
            partition_type="HASH"
            distribution_type['dist_mode'] = dist_mode
            distribution_type['partition_type'] = partition_type
        elif re.compile('\w*UNPARTITIONED\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "UNPARTITIONED"
            distribution_type['dist_mode'] = dist_mode
            distribution_type['partition_type'] = partition_type
        elif re.compile('\w*RANDOM\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANDOM"
            distribution_type['dist_mode'] = dist_mode
            distribution_type['partition_type'] = partition_type
        elif re.compile('\w*RANGE\w*').match(dist_type):
            dist_mode = "PARTITIONED"
            partition_type = "RANGE"
            distribution_type['dist_mode'] = dist_mode
            distribution_type['partition_type'] = partition_type
        elif re.compile('\w*BROADCAST\w*').match(dist_type):
            dist_mode = "BROADCAST"
            partition_type = None
            distribution_type['dist_mode'] = dist_mode
        else:
            self.logging.error('no dist type:%s'%(dist_type))
        return distribution_type
    
    def get_attri(self):
        return self.attri_dic