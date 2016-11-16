from kibana import Kibana
import sys, traceback
from logger import Logger
import json
from datetime import date, timedelta


class KibanaDaily:
    def __init__(self):
        self.logger = Logger("daily-"+str(date.today()))


    def _read_config(self):
        cfg = open('../config/kibana_new.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        return cfg_data['names']


    def import_data(self):
        try:
            #yesterday = '2016-10-22'
            yesterday = date.today() - timedelta(1)
            names = self._read_config()
            kibana = Kibana(self.logger.logln)
            for name in names:
                kibana.import_data( str(yesterday), name, 0, True )
            self.logger.logln("SUCCESS.")
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.logln("Exception " + str(e))
            for line in traceback.format_tb(exc_traceback):
                self.logger.log(line)


daily = KibanaDaily()
daily.import_data()
