from kibana import Kibana
import sys, traceback
from logger import Logger
import json
from datetime import date, timedelta


class KibanaDaily:
    def __init__(self):
        self.logger = Logger("daily-"+str(date.today()))


    def _read_config(self):
        cfg = open('../config/kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        return cfg_data['products']


    def import_data(self):
        try:
            yesterday = date.today() - timedelta(1)
            products = self._read_config()
            kibana = Kibana(self.logger.logln)
            for product in products:
                kibana.import_data( str(yesterday), product, 0, True )
            self.logger.logln("SUCCESS.")
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.logln("Exception " + str(e))
            for line in traceback.format_tb(exc_traceback):
                self.logger.log(line)


daily = KibanaDaily()
daily.import_data()
