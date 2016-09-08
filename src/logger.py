import sys
from datetime import time, datetime


class Logger:
    def __init__(self, name):
        self.name = '../logs/' + name + '.log'
        self.f = open(self.name, 'a', 0)
        
    def _log(self, message):
        time = datetime.now().strftime("%H:%M:%S")
        message = '['+time+'] '+message

        sys.stdout.write(message)
        self.f.write(message)
        
        
    def log(self, message):
        self._log(message)

    def logln(self, message):
        self._log(message + '\n')
        
