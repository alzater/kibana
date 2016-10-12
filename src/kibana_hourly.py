from kibana import Kibana
import sys, traceback
from logger import Logger
import json
from datetime import date, timedelta
from time import strftime


class KibanaHourly:
    def __init__(self):
        self.logger = Logger("hourly-"+str(date.today())+"-"+str(strftime("%H:%M:%S")))


    def _read_config(self):
        names = ['glads2_mm', 'glads2_gp', 'glads2_vk']

        return names


    def import_data(self):
        try:
            names = self._read_config()
            kibana = Kibana(self.logger.logln)
            for name in names:
                kibana.import_data( str(date.today()), name, 0, True )
            self.logger.logln("SUCCESS.")
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.logln("Exception " + str(e))
            for line in traceback.format_tb(exc_traceback):
                self.logger.log(line)


hourly = KibanaHourly()
hourly.import_data()
