from xml.dom.minidom import *

class xml_node:
    def __init__(self,dom):
        self.dom = dom
        
    def createNode(self,tagName,attri={},text=None):
        item = self.dom.createElement(tagName)
        
        if len(attri.keys()) > 0:
            for key in attri.keys():
                item.setAttribute(key, attri[key])
        
        if text is not None:
            textNode = self.dom.createTextNode(text)
            item.appendChild(textNode)
            
        return item
    
class xml_writers:
    def __init__(self,xml_file,query_name):
        self.impl = getDOMImplementation()
        self.xml_file = xml_file
        self.query_name = query_name
        
    def create_dom(self):
        self.dom = self.impl.createDocument(None, 'execution_plan', None)
        self.root = self.dom.documentElement
        self.root.setAttribute('query',self.query_name)
        return self.dom
        
    def root_append(self,item):
        self.root.appendChild(item)
        
    def print_self(self):
        print (self.root.toprettyxml())
    
    def save_xml(self):
        self.root.toprettyxml()
        fp = open(self.xml_file,'w')
        self.dom.writexml(fp, addindent='\t', newl='\n')
        fp.close()
        

if __name__ == '__main__':
    obj = xml_writers('./tt.xml')
    dom = obj.create_dom()
    obj_node = xml_node(dom)
    node_item1 = obj_node.createNode('hehe',{'pre_name':'li','name':'xiao',},'lixiao')
    node_item1_1 = obj_node.createNode('haha',{'pre_name':'wang','name':'de',},'xw')
    node_item1.appendChild(node_item1_1)
    obj.root_append(node_item1)
#    obj.createNode('hehe',{'pre_name':'li','name':'xiao',},'lixiao')
#    obj.createNode('haha',{'pre_name':'wang','name':'de',})
    obj.save_xml()
    
                