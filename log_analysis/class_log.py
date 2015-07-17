from getFragmentInfo import FragmentInfo
class class_log:

    def __init__(self, log_file):
        fp = open(log_file, 'r')
        self.logfile = fp.read()
        self.part_impalad = (0, self.logfile.find('+----------+'))
        self.part_queryResult = (self.part_impalad[1], self.logfile.find('Fetched', self.part_impalad[1] + 1))
        self.part_queryRuntime = (self.part_queryResult[1], self.logfile.find('----------------', self.part_queryResult[1] + 1))
        self.part_planFragment = (self.part_queryRuntime[1], self.logfile.find('----------------', self.part_queryRuntime[1] + 1))
        self.part_eachnodecosts = (self.part_planFragment[1], self.logfile.find('    Coordinator Fragment', self.part_planFragment[1] + 1))
        self.part_plannode = (self.part_eachnodecosts[1], len(self.logfile))
        fp.close()
        fp_t = open('hehe.txt', 'w')
        print >> fp_t, self.logfile[self.part_planFragment[0]: self.part_planFragment[1]] 
        fp_t.close()
#         self.attribute = {}
#         fp.close()

    def getAttri(self):
        FragmentInfo_obj = FragmentInfo(self.logfile[self.part_planFragment[0] : self.part_planFragment[1]])
        FragmentInfo_obj.getAttri()

if __name__ == '__main__':
    obj = class_log('./log_dir/q19.sql.log')
    obj.getAttri()
    
                
