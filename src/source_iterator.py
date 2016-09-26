import time

class SourceIterator:
    def __init__(self, iter_type, start):
        self.iter_type = iter_type
        self.start = start
        
        self.current = start


    def get(self):
        return self.current
        
        
    def get_str(self):
        if self.iter_type == 'id':
            return str(self.current)
        if self.iter_type == 'HH:MM:SS':
            return self._get_time_str()
        return ''
        
        
    def get_param(self):
        if self.iter_type == 'id':
            return "&id="+ str(self.current)
        if self.iter_type == 'HH:MM:SS':
            return "&beg_time="+ self._get_time_str()
        return ''
        
              
    def set(self, last):
        if self.iter_type == 'HH:MM:SS':
            self.current = _get_time_from_str(last) + 1
            return
        if self.iter_type == 'id':
            self.current = int(last) + 1
            return
        
        
    def clear(self):
        self.current = self.start
        
     
    def _get_time_str(self):
        time = self.current
        res = ''
        res = str(time % 60)
        time /= 60
        res = str(time % 60) + ':' + res
        time /= 60
        res = str(time) + ':' + res  
        return res  
    
    
    def _get_time_from_str(self, time_str):
        res = 0
        splited = time_str.split(':')
        res += int(splited[0] * 60 * 60)
        res += int(splited[1] * 60)
        res += int(splited[2])
        return res
        
        
        


