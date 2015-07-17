class getImpalaInfo:
    def __init__(self,log_lines):
        self.logfile = log_lines
    
    def getLineInfo(self):
        a=0
    def getMultiLineInfo(self):
        b=0    
    def getInfo(self):
        for i, line in enumerate(self.logfile):
            if line.find('Connected to ') != -1:
                self.attribute['Impalad'] = line[len('Connected to') + 1:]
                continue
            
            if line.find('Server version:') != -1:
                self.attribute['server_version'] = line[len('Server version:') + 1 : ]
                continue
            
            if line.find('Query: use ') != -1:
                self.attribute['db_name'] = line[len('Query: use') + 1 : ]
                continue
            
            if line.find('Query: select') != -1:
                
    def getImpalad(self):
        for i, line in enumerate(self.logfile):
            if line.find('Connected to ') != -1:
                impalad = line[len('Connected to ') + 1:]
                self.logfile = self.logfile[i + 1:]
                self.attribute['Impalad'] = impalad
    def getServer(self):
        for i,line in enumerate(self.logfile):
            if line.find('Server version:') != -1:
                self.attribute['server_version'] = line['Server version:' + 1 : ]
                self.logfile = self.logfile[i + 1:]
                
    def getDBname(self):
        for i,line in enumerate(self.logfile):
            if line.find('Server version:') != -1:
                self.attribute['server_version'] = line['Server version:' + 1 : ]
                self.logfile = self.logfile[i + 1:]     
    def getQuery(self):
        for i,line in enumerate(self.logfile):
            if line.find()!= -1:
    