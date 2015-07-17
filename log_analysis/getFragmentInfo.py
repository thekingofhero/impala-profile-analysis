class FragmentInfo:
    def __init__(self, logfile):
        self.logfile = logfile
    
    def getAttri(self):
        fragments = self.getFragments()
        print fragments
        
    def getFragments(self):
        fragments = self.logfile.split('\n\n')[1:]
#         for fragment in fragments:
            
        
    