from kibana import Kibana
import sys, traceback
from logger import Logger
import json
from datetime import date, timedelta
from time import strftime, time, gmtime


class KibanaHourly:
    def __init__(self):
        self.logger = Logger("hourly-"+str(date.today())+"-"+str(strftime("%H:%M:%S")))


    def _read_config(self):
        names = ['glads2_mm', 'glads2_gp', 'glads2_vk']

        return names





    def import_data(self):
        try:
            cur_time = gmtime(time() - 1*60*60 + 10)
            names = self._read_config()
            kibana = Kibana(self.logger.logln)
            kibana.set_beg_time( strftime('%H:00:00', cur_time) )
            kibana.set_end_time( strftime('%H:59:59', cur_time) )
            
            recreate_index = True
            if int(strftime('%H')) < 10:
                kibana.set_beg_time( None )
            else:
                recreate_index = False
            
            for name in names:
                kibana.import_data( str(date.today()), name, 0, recreate_index )
            self.logger.logln("SUCCESS.")
        except BaseException as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.logger.logln("Exception " + str(e))
            for line in traceback.format_tb(exc_traceback):
                self.logger.log(line)


hourly = KibanaHourly()
hourly.import_data()
