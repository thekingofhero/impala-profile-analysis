import os
import sys
import re
import string

query = sys.argv[1]
#query='q59'

def split_profile_xml():
    homedir = os.getcwd()
    i = 0  
    n = 0 
    input_file = open(homedir + '\\' + query +'.log','r')
    out_file = open(homedir + '\\' + 'tmp_1.xml','w')
    out_file.write('<execution_plan query="' + query +'">' + '\n')
    while True:
        while True :
            line = input_file.readline()
            if line.find('PLAN FRAGMENT') != -1 :
                i = i + 1
                if i == 1 :
                    out_file.write('  <plan_fragment fid="' + str(int(line[1:3])) +  '" is_root="true">' + '\n')
                    continue
                out_file.write('  <plan_fragment fid="' + str(int(line[1:3])) +  '" is_root="false">' + '\n')
                continue
            if line.find('DATASTREAM') != -1 :
                datastream = line
                datastream = re.sub(r',', ' ', datastream)
                datastream = re.sub(r'=', ' ', datastream)
                datastream = re.sub(r'F', ' ', datastream)
                datastream = re.sub(r']', ' ', datastream)
                datastream = re.sub(r'\(', ' ', datastream)
                datastream = datastream.split(' ')
                if datastream[12] == 'UNPARTITIONED' :
                    out_file.write('        <data_stream_sink fid="' + datastream[7] + '" nid="' + datastream[10] + '" dist_mode="PARTITIONED" partition_type="'+datastream[12]+'" />' + '\n')
                if datastream[12] == 'HASH' :
                    out_file.write('        <data_stream_sink fid="' + datastream[7] + '" nid="' + datastream[10] + '" dist_mode="PARTITIONED" partition_type="HASH" />' + '\n')
                if datastream[12] == 'BROADCAST' :
                    out_file.write('        <data_stream_sink fid="' + datastream[7] + '" nid="' + datastream[10] + '" dist_mode="BROADCAST" />' + '\n')
            pattern_limit = re.compile(r' *limit: \d*')
            match_limit = pattern_limit.match(line)
            if match_limit :
                limit = match_limit.group()
                limit = limit.split(' ')
                lim = limit[6]               
            pattern_tup_ids = re.compile(r'.*tuple-ids=.*')
            match_tup_ids = pattern_tup_ids.match(line)                            
            pattern = re.compile(r'  \d*:\D* *')
            match = pattern.match(line)
            if match:
                n += 1
                fid = match.group()
                if fid[5:21] == 'MERGING-EXCHANGE' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="'+fid[5:21]+'" tuple-ids="14" row-size="168" limit="100" dist_mode="PARTITIONED" partition_type="UNPARTITIONED">' + '\n')
                    out_file.write('            <rows_returned>149</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n')  
                if fid[5:10] == 'TOP-N' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="false" display_name="'+ fid[5:10]+'" children="16" tuple-ids="14" row-size="168" limit="100">' + '\n')  
                    out_file.write('            <rows_returned>100</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n')
                if fid[5:14] == 'HASH JOIN' :
                    if line.find('BROADCAST') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="false" display_name="' + fid[5:14] + '" children="28,29" tuple-ids="2,4,5,9,11,12" row-size="236" join_type="INNER JOIN" dist_mode="BROADCAST">' + '\n')  
                        out_file.write('            <rows_returned>45612</rows_returned>' + '\n')
                        out_file.write('            <probe_rows>6570</probe_rows>' + '\n')
                        out_file.write('            <build_rows>6588</build_rows>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                    else :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="false" display_name="' + fid[5:14] + '" children="28,29" tuple-ids="2,4,5,9,11,12" row-size="236" join_type="INNER JOIN" dist_mode="PARTITIONED">' + '\n')  
                        out_file.write('            <rows_returned>45612</rows_returned>' + '\n')
                        out_file.write('            <probe_rows>6570</probe_rows>' + '\n')
                        out_file.write('            <build_rows>6588</build_rows>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                if fid[5:13] == 'EXCHANGE' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="false" display_name="' +fid[5:13] + '" children="" tuple-ids="9,11,12" row-size="108" dist_mode="PARTITIONED" partition_type="HASH">' + '\n')
                    out_file.write('            <rows_returned>6570</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n')
                if fid[5:14] == 'SCAN HDFS' :
                    if line.find('tpcds_text') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:14] + '" database_name="tpcds_text" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                    if line.find('tpcds_parquet') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:14] + '" database_name="tpcds_parquet" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                    if line.find('tpcds_parquet_snappy') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:14] + '" database_name="tpcds_parquet_snappy" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')          
                if fid[5:14] == 'AGGREGATE' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:14] + '" children="10" tuple-ids="9" row-size="64">' + '\n')
                    out_file.write('            <rows_returned>17866</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n')
                if fid[6:10] == 'HDFS' :
                    if line.find('tpcds_text') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[6:10] + '" database_name="tpcds_text" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                    if line.find('tpcds_parquet') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[6:10] + '" database_name="tpcds_parquet" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')
                    if line.find('tpcds_parquet_snappy') != -1 :
                        out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[6:10] + '" database_name="tpcds_parquet_snappy" table="date_dim_text" partitions="1/1" tuple-ids="5" row-size="8">' + '\n')
                        out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                        out_file.write('        </plan_node>' + '\n')                    
                if fid[5:13] == 'ANALYTIC' : 
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:13] + '" children="10" tuple-ids="9" row-size="64">' +'\n')
                    out_file.write('            <rows_returned>365</rows_returned>' +'\n')
                    out_file.write('        </plan_node>' + '\n')
                if fid[5:9] == 'SORT' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:9] + '" children="10" tuple-ids="9" row-size="64">' + '\n')
                    out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n')
                if fid[5:11] == 'SELECT' :
                    out_file.write('        <plan_node nid="' + fid[2:4] + '" is_plan_root="true" display_name="' + fid[5:11] + '" children="10" tuple-ids="9" row-size="64">' + '\n')
                    out_file.write('            <rows_returned>365</rows_returned>' + '\n')
                    out_file.write('        </plan_node>' + '\n' )
            pattern1 = re.compile(r'  \|--\d*:\D* ')
            match1 = pattern1.match(line)
            if match1 :
                nid = match1.group()
                if line.find('HASH') != -1 :
                    out_file.write('        <plan_node nid="' + nid[5:7] + '" is_plan_root="false" display_name="' + nid[8:16] + '" children="" tuple-ids="9,11,12" row-size="108" dist_mode="PARTITIONED" partition_type="HASH">' + '\n') 
                    out_file.write('            <rows_returned>6588</rows_returned>' + '\n')
                    out_file.write('        </plan_node>        ' + '\n')               
                else :
                    out_file.write('        <plan_node nid="' + nid[5:7] + '" is_plan_root="false" display_name="' + nid[8:16] + '" children="" tuple-ids="9,11,12" row-size="108" dist_mode="BROADCAST">' + '\n') 
                    out_file.write('            <rows_returned>6588</rows_returned>' + '\n')
                    out_file.write('        </plan_node>        ' + '\n')
            pattern2 = re.compile(r'\s{5}tuple-ids.*')
            match2 = pattern2.match(line)
            if match2 :
                node_end = match2.group()
                out_file.write('  </plan_fragment>' + '\n')
            if not line :
                break      
        if not line :
            break
    out_file.write('</execution_plan>' + '\n')
    
    
    
    
    
def split_profile_x():
    import os
    import sys
    import re
    import string
    instances_num = 1
    homedir = os.getcwd()
    return_list = []
    pro_list = []
    build_list = []
    homedir = os.getcwd()
    i = 0 ; n =0 ;y = 0
    input_file = open(homedir + '\\' + query + '.log','r')
    input_xml = open(homedir + '\\' + 'tmp_1.xml','r')
    out_file = open(homedir + '\\' + 'tmp_2.xml','w')
    while True :
        line = input_file.readline()
#        print line
        line = re.sub(r',', ' ', line)
        line = re.sub(r'\)',' ', line)
        
        if line.find('num instances:') != -1 :
            
            inst_num = line.strip('\n')
            inst_num = inst_num.split(' ')
            instances_num = int(inst_num[8])
   #         print instances_num,'$$$$$$$$$$$'
        
        

        if line.find('RowsReturned:') != -1 :
            line = re.sub(r'\(',':', line)
            line=line.strip('\n')
            
#            print line
            re_row =line
            re_row = re_row.split(':')
            re_rows = re_row[-1:]
  #          print int(re_rows[0]) ,'^^^^^^^^^^^^^'
            re_rows2 = int(re_rows[0]) * instances_num
            re_rows2 = str(re_rows2)
  #          print re_rows2 ,'XXXXXXXXXXXXXXXXXXXXX'
            return_list.append(re_rows2)
  #          print return_list
            
        if line.find('ProbeRows:') != -1 :
#            print line
            pro_line = line.strip('\n')
            pro_line = re.sub('\:','(',pro_line)
            pro_line = pro_line.split('(')
#            pro_line=pro_line.strip('\n')
            pro_rows = pro_line[-1:]
            pro_rows2 = int(pro_rows[0]) * instances_num
            pro_rows2 = str(pro_rows2)
 #           print pro_rows2 ,'VVVVVVVVVVVVVVVVVVVVV'
            pro_list.append(pro_rows2)
               
        if line.find('BuildRows:') != -1 :
            line = re.sub(r'\(',':', line)
            line=line.strip('\n')
            build_row = line.split(':')
            build_rows = build_row[-1:]
            build_rows2 = int(build_rows[0]) * instances_num
            build_rows2 = str(build_rows2)
            build_list.append(build_rows2)
#            print build_row[-1:]
        if line.find('    Fragment F') != -1 : 
            break
#    print  'rows_return_list' ,return_list
#    print  'proberows_list',pro_list
#    print  'buildrows_list',build_list
    n = 0
    while True :
        
        line = input_xml.readline()
        
#        pattern = re.compile(r'.*tuple-ids=".*\d*">*')
        pattern = re.compile(r'.*<probe_rows>\d*</probe_rows>.*')
        match = pattern.match(line)
        if match :
            pro_value = pro_list[n]
            pro_value=pro_value.strip(' ')
            line = re.sub(r'\d{1,100}',pro_value,line)    
#            print line + '*****************'
            out_file.write(line)
            n  += 1
            continue
        pattern1 = re.compile(r'.*<build_rows>\d*</build_rows>.*')
        match1 = pattern1.match(line)
        if match1 :
            build_value = build_list[i]
            build_value = build_value.strip(' ')
            line = re.sub(r'\d{1,100}',build_value,line)
#            print line
            i += 1 
#            print line
            out_file.write(line)
            continue
        pattern2 = re.compile(r'.*<rows_returned>\d*</rows_returned>.*')
        match2 = pattern2.match(line)
        if match2 :
            return_value = return_list[y]
#            print return_value , 'BBBBBBBBBBBBBBBBB'
            return_value = return_value.strip(' ')
            
#            print line
            line = re.sub(r'\d{1,100}',return_value,line)
#            print line
            y += 1 
#            print line
            out_file.write(line)
            continue
        out_file.write(line) 
        if not line : 
            break
            
            
def find_value():
    tupleid = None
    size = None
    Limit = None
    size_list = []
    ids_list = []
    table_list = []
    part_list = []
    homedir = os.getcwd()
    i = 0 ; n =0 ; z = 0 ; x = 0;
    input_file = open(homedir + '\\' + query + '.log','r')
    input_xml1 = open(homedir + '\\' + 'tmp_2.xml','r')
    out_file = open(homedir + '\\' + 'tmp_3.xml','w')
    while True :
        line = input_file.readline()
        pattern_tup_ids = re.compile(r'.*tuple-ids=.*')
        match_tup_ids = pattern_tup_ids.match(line)
        if match_tup_ids :
            list = match_tup_ids.group()
            list = re.sub(r'row-size',' ',line)
            list = re.sub(r'cardinality',' ',list)
            list = re.sub(r'B',' ',list)
            list = list.split('=')
            tupleid = str(list[1])
            tupleid=tupleid.strip(' ')
            size = int(list[2])
            size_list.append(size)
            ids_list.append(tupleid)
        pattern_limit = re.compile(r' *limit: \d*')
        match_limit = pattern_limit.match(line)
        if match_limit :
            limit = match_limit.group()
            limit = limit.split(' ')
            lim = limit[6]               
        pattern_Limit = re.compile(r'.*LIMIT=\d*.*')
        match_Limit = pattern_Limit.match(line)
        if match_Limit :
            Limit = match_Limit.group()
            Limit = re.sub(r']',' ',Limit)
            Limit = Limit.split('=')   
        pattern = re.compile(r'  \d*:\D* *')
        match = pattern.match(line)
        if match:
            n += 1
            fid = match.group()
            fid =fid.split(":")
        pattern1 = re.compile(r'  \|--\d*:\D* ')
        match1 = pattern1.match(line)
        if match1 : 
            fid2 =match1.group()
            fid2 =re.sub(r'-',' ',fid2)
            fid2 =re.sub(r'\|',' ',fid2)
            fid2 =fid2.split(':')
        pattern_table = re.compile(r'.*\[tpcds_.*')
        match_table = pattern_table.match(line)
        if match_table :
#            print match_table.group()
            table = match_table.group()
            table = re.sub(r',',' ',table)
            table = re.sub(r'\.',' ',table)
            table = table.split(' ')
            tab = table[5]
#            print tab
            table_list.append(tab)
        pattenr_patt =re.compile(r' *partitions=\d*/\d*.*')
        match_patt = pattenr_patt.match(line)
        if match_patt :
#            print match_patt.group()
            patt = match_patt.group()
            patt = re.sub(r'=',' ',patt)
            patt = patt.split(' ')
#            print patt[6]
            part =patt[6]
            part_list.append(part)        
        if  line.find('Operator') != -1   :
            break
    while True : 
        line = input_xml1.readline()
        pattern = re.compile(r'.*tuple-ids="*.*')
        match = pattern.match(line)
        if match :
            ids_value = 'tuple-ids=\"' +ids_list[z] + '" row'
            
            line = re.sub(r'tuple-ids=\".*" row',ids_value,line)
            size_value = 'row-size="' + str(size_list[z]) +'"'
            line = re.sub(r'row-size=\"\d*"',size_value,line)
            if line.find('table') != -1:
                part_value = 'partitions="' + part_list[x] +'" tup'
                table_value = 'table="' + table_list[x] + '" pa'
                line = re.sub(r'partitions=".*" tup',part_value,line)
                line = re.sub(r'table=".*" pa',table_value,line)
                x += 1
            out_file.write(line)
            z += 1
            continue
        out_file.write(line)    
        if not line :
            break
            
            
            

def find_children():
    child = None ; value = None ; Value = None
    homedir = os.getcwd()
    n = 0 ; node = [] ; children = [] ;x = 0 ;i =0 ;j = 0
    node1 = [] ; children1 = []
    input_file = open(homedir + '\\' + query + '.log','r')
    input_file2 = open(homedir + '\\' + 'tmp_3.xml','r')
    out_file = open(homedir + '\\' + 'tmp_4.xml','w')
    while True:
        line = input_file.readline()
        line = line.strip('\n')
        line = line.strip(' ')
        if line.find('PLAN FRAGMENT') != -1 :
            j += 1
        for value in re.findall(r'^\d\d',line) :
            if value != -1 and j > 0 :
                value = value.strip(' ')
                value = value.strip('--')
                n += 1        
                if len(value) == 0 :
                    continue 
                if n > 1 :
                    node1.append(Value)
                    children1.append(value)
                if line.find('EXCHANGE') != -1:
                    n = 0
                if line.find('SCAN HDFS') != -1:
                    n = 0
                Value = value
        for child in re.findall(r'--\d\d',line):
            child = child.strip('--')
            node.append(value)
            children.append(child)
        if line.find('Operator') != -1 :
            break
#    print children 
#    print children1
    while True :
        line = input_file2.readline()
        line = line.strip('\n')
#        print line
        if x < len(node1) :
            if line.find('nid="'+str(node1[x])) != -1 :
                line = re.sub(r'children="\d*','children="'+str(children1[x]),line)
#                print children1[x]
                x += 1
        if i < len(node):
            if line.find('nid="'+str(node[i])) != -1 :
                line = re.sub(r'\d*" tuple',str(children[i]+'" tuple'),line)
#                print children[i]
                i += 1
        out_file.write(line +'\n')
        if not line :
            break




def check_root():
    homedir = os.getcwd()
    i = 0 
    input_file = open(homedir + '\\' + 'tmp_4.xml','r')
    out_file = open(homedir + '\\' + 'tmp_5.xml','w')
    while True :
        line = input_file.readline()
        line = str(line.strip('\n'))
        if line.find('plan_fragment fid=') != -1 :
            if i < 1 :
                line = re.sub(r'false','true',line)
            if i > 1 :
                line = re.sub(r'true','false',line)
            i += 1
            n = 0
        if line.find('plan_node nid=') != -1 :
            if n == 0 :
               line = re.sub(r'false','true',line)
            if n > 0 :
                line = re.sub(r'true','false',line)
            n += 1
        out_file.write(line + '\n')
        if not line :
            break


def str_int():
    homedir = os.getcwd()
    input_file = open(homedir + '\\' + 'tmp_5.xml','r')
    out_file = open(homedir + '\\' + query + '.xml','w')
    while True :
        line = input_file.readline()
        line = str(line.strip('\n'))
        if line.find('plan_node nid=') != -1 :
            line1 = line.split('"')
            node_id = 'nid="' + str(int(line1[1])) + '"'            
            line = re.sub(r'nid="\d*"',node_id,line) 
        if line.find('fid=') != -1 :
            lin0 = line.split('"')
            fg_id = 'fid="' + str(int(lin0[1])) + '"'
            line =re.sub(r'fid="\d*"',fg_id,line)
        if line.find('children=') != -1:
            line2 =line.split('"')
            if 3 > len(line2[7]) > 0 :
                child_id = 'children="'+ str(int(line2[7])) + '"'
                line = re.sub(r'children="\d*"',child_id,line)
            if len(line2[7]) > 3 :
                child_id2 = line2[7]
                child_id2 = child_id2.split(',')               
                child = 'children="'+str(int(child_id2[0]))+','+str(int(child_id2[1]))  + '"'
                line = re.sub(r'children="\d*,\d*"',child,line)
        out_file.write(line + '\n')
        if not line :
           break
           
def get_rowsread(queryname):
    i = 0 ; n = 0 ;x =0 
    msg = {}
    pj_path = '.'
    log_dir = '.'
    f = open(os.path.join(log_dir,r'%s.log'%(queryname)),'r')
    while True :
        line = f.readline()
        instan_num = re.search('num instances: (\d{1,3})',line)
        node = re.search('HDFS_SCAN_NODE \(id=(\d{1,3})',line)
        rows1 = re.search('RowsRead: .*\((\d{1,8})\)\n',line)
        rows2 = re.search('RowsRead: (\d{1,8})\n',line)
        if instan_num :
            n = instan_num.groups()[0]
        elif node :
            i = node.groups()[0] 
        elif rows1 :
            x = rows1.groups()[0]
            x = str(int(n)*int(x))
            key_nid = 'nid="%s"' % (i )
            value_rows = '<rows_read>%s</rows_read>' % (x )
            msg[key_nid] = value_rows
 #           print 'nid="%s" <rows_read>"%s"</rows_read>' % (i , x)
        elif rows2 :
            x = rows2.groups()[0]
            x = str(int(n)*int(x))
            key_nid = 'nid="%s"' % (i )
            value_rows = '<rows_read>%s</rows_read>' % (x )
            msg[key_nid] = value_rows
#            print 'nid="%s" <rows_read>"%s"</rows_read>' % (i , x)
        elif line.find('    Fragment F') != -1  :
            break
    return msg
    f.close()  
    
def insert_readmsm(queryname):
    msg = {} ; x = 0
    msg = get_rowsread(queryname)
    pj_path = '.'
    xml_dir = '.'
    f = open(os.path.join(xml_dir,r'%s.xml'%(queryname)),'r')
    f_des = open(os.path.join(xml_dir,r'%s.xml_new'%(queryname)),'w')
    while True :
        line = f.readline()
        line = line.strip('\n')
        
        for i in msg.keys() :
            if line.find(i) != -1 :
                x = 1
                break
            
        f_des.write(str(line+'\n'))
#        print line
        if x == 1 :
            f_des.write(str('            ' + msg[i] + '\n'))
#            print '            ' + msg[i]
        x += 1
        if len(line) == 0 :
            break
    f.close()
    f_des.close()
    os.system('move %s %s'%(os.path.join(xml_dir,r'%s.xml_new'%(queryname)),
                                              os.path.join(xml_dir,r'%s.xml'%(queryname))))
               
           
if __name__ == '__main__':
    split_profile_xml()
    split_profile_x()
    find_value() 
    find_children()
    check_root()
    str_int()
    insert_readmsm(query)          