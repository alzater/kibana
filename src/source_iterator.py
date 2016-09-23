class SourceIterator:
    def __init__(self, iter_type, start):
        self.iter_type = iter_type
        self.start = start
        
        self.current = start


    def get(self):
        return self.current
        
        
    def getStr(self):
        if self.iter_type == 'id':
            return "&id="+ self.current
        return ''
        
        
    def set(self, last_id):
        self.current = last_id + 1
        
        
    def clear(self):
        self.current = self.start


