from class_xml.xml_writer import xml_writers,xml_node
from xml.dom.minidom import *

from getFragmentInfo import FragmentInfo

class class_log:

    def __init__(self, log_file,xml_filename):
        self.xml_filename = xml_filename
        fp = open(log_file, 'r')
        self.logfile = fp.read()
        self.part_impalad = (0, self.logfile.find('+----------+'))
        self.part_queryResult = (self.part_impalad[1], self.logfile.find('Fetched', self.part_impalad[1] + 1))
        self.part_queryRuntime = (self.part_queryResult[1], self.logfile.find('----------------', self.part_queryResult[1] + 1))
        self.part_planFragment = (self.part_queryRuntime[1], self.logfile.find('----------------', self.part_queryRuntime[1] + 1))
        self.part_eachnodecosts = (self.part_planFragment[1], self.logfile.find('    Coordinator Fragment', self.part_planFragment[1] + 1))
        self.part_plannode = (self.part_eachnodecosts[1], len(self.logfile))
        fp.close()
#         fp_t = open('hehe.txt', 'w')
#         print >> fp_t, self.logfile[self.part_planFragment[0]: self.part_planFragment[1]] 
#         fp_t.close()
#         self.attribute = {}
#         fp.close()

    def getAttri(self):
        FragmentInfo_obj = FragmentInfo(self.logfile[self.part_planFragment[0] : self.part_planFragment[1]])
        self.infoTOwrite = FragmentInfo_obj.getAttri()


    def writeToXML(self):
        obj_xml = xml_writers('./output_dir/%s'%(self.xml_filename))
        dom = obj_xml.create_dom()
        obj_node = xml_node(dom)
        for fragment in self.infoTOwrite:
            fg_node = obj_node.createNode('plan_fragment',{'fid':fragment.fid(),\
                                                           'is_root':fragment.is_root()})
            
            for plannode in fragment['plannodes']:
        
        node_item1 = obj_node.createNode('hehe',{'pre_name':'li','name':'xiao',},'lixiao')
        node_item1_1 = obj_node.createNode('haha',{'pre_name':'wang','name':'de',},'xw')
        node_item1.appendChild(node_item1_1)
        obj_xml.root_append(node_item1)
    #    obj.createNode('hehe',{'pre_name':'li','name':'xiao',},'lixiao')
    #    obj.createNode('haha',{'pre_name':'wang','name':'de',})
        obj_xml.save_xml()
        return 
                
