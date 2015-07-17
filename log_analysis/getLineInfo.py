class getLineInfo:
    def __init__(self, log_lines):
        self.logfile = log_lines
    def getLineInfo(self, key, keywords):
        attribute = {}
        for i, line in enumerate(self.logfile):
            if line.find(keywords) != -1:
                impalad = line[len(keywords) + 1:]
                self.logfile = self.logfile[i + 1:]
                attribute[key] = impalad
        return attribute
