from kibana import Kibana
from logger import Logger
from datetime import datetime
import sys


class KibanaManual:
    def __init__(self):
        self.logger = Logger("manual-"+str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))


    def import_data(self):
        if len(sys.argv) < 2:
            self.logger.logln("no product")
            return

        product = sys.argv[1]

        start_id = 0
        if len(sys.argv) >= 3:
            start_id = int(sys.argv[2])

        recreate_index = True
        if len(sys.argv) >= 4:
            if sys.argv[3] == "1" or sys.argv[3] == "True" or sys.argv[3] == "true":
                recreate_index = True
            else:
                recreate_index = False

        dates = []
        if len(sys.argv) <= 4:
            dates.append( str(datetime.now().date()) )
        else:
            i = 4
            while i < len(sys.argv):
                dates.append( str(sys.argv[i]) )
                i += 1

        if len(dates) == 0:
            self.logger.logln("no dates")
            return

        kibana = Kibana(self.logger.logln)
        for date in dates:
            kibana.import_data(date, product, start_id, recreate_index)

kibana = KibanaManual()
kibana.import_data()
