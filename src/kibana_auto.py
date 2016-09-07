from kibana import Kibana

import sys
import json
from time import sleep
from datetime import datetime, date, time

class Kibana_auto:
    def __init__(self):
        self.time = time(4)
        if len(sys.argv) >= 2:
            self.time = time(int(sys.argv[1]))

        self.date = datetime.now().date()
        self.read_config()

        while True:
            self.loop()

    def loop(self):
        if self.date == datetime.now().date():
            print "sleep", datetime.today().time()
            sleep(60*60)
            return

        if self.time > datetime.today().time():
            print "sleep", datetime.today().time()
            sleep(60*60)
            return

        kibana = Kibana()
        
        recreate_index = True
        for product in self.products:
            kibana.import_data( str(self.date), product, 0, recreate_index )
            recreate_index = False

        self.date = datetime.today().date()

    def read_config(self):
        cfg = open('../config/kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.products = cfg_data['products']

kibana = Kibana_auto()

