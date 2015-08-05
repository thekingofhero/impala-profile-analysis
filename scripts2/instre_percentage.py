import os

inputxml_file_path = r'E:\impala_simulator\resource\profiles\text\profiles\execution_plans'
outputxml_file_path = r'E:\impala_simulator\resource\profiles\text\profiles\execution_planstmp'
dp_file_path = r'E:\impala_simulator\resource\profiles\text\data_percentage\data_percentage.csv'

# inputxml_file_path = r'C:\Development\logs\generated_data\text\execution_plans'
# outputxml_file_path = r'C:\Development\logs\generated_data\text\execution_plans\tmp'
# dp_file_path = r'C:\Development\logs\generated_data\text\execution_plans\data_percentage.csv'


def retest():

    if not os.path.exists(outputxml_file_path):
        os.mkdir(outputxml_file_path)

    import string
    import re
    from re import findall 
    input_file = open(dp_file_path,'r')
    i = 0 ; j=0 ;n = 0 ; dict = {} ; mun1 = None
    plan_list = [] ; nid_list = [] ; perce_list = []
    while True : 
        line=input_file.readline()
        line = line.strip('\n')
        line = re.sub(r',',' ',line)
        str1 = findall(r'q.*\.sql.xml|s.*\.sql.xml',line)
        if str1 != -1  :
            if len(str1) > 0:
                plan_list.append(str1[0])
        nid = findall(r'id: \d*',line)
        if nid != -1 :
            if len(nid) > 0 :
                nodeid = nid[0]
                nodeid = re.sub(r'id: ','nid="',nodeid)
                nedeid = nodeid.split('"')
                num = int(nedeid[1])
                num1 = nedeid[0]
                nid2 = num1 + str(num)
                nid2 = re.sub('=','="',nid2)
                nid_list.append(nid2)
        perce = findall(r'percentage: .*\d',line)
        if nid != -1 :
            if len(perce) > 0 :
                perce = re.sub(': ','="',perce[0])
                perce = perce[len('percentage="'):]
                perce_list.append('data_percentage="'+perce + '"')
                
        if not line :
   #         print plan_list ,'\n' , nid_list ,'\n' , perce_list
            break
    while True : 
        inputxml_file = open(os.path.join(inputxml_file_path, plan_list[i]),'r')
        outputxml_file = open(os.path.join(outputxml_file_path, plan_list[i]),'w')
        while True :
            linex = inputxml_file.readline()
            linex = linex.strip('\n')
            if linex.find(nid_list[i]+'"') != -1 :
                linex = re.sub('>',' '+ perce_list[i] + '>',linex)
#            print linex
            outputxml_file.write(linex + '\n')
            if not linex :
                break
        i += 1
        if i == len(plan_list) :
            break
    input_file.close( )
    inputxml_file.close( )

if __name__ == '__main__':
    retest()