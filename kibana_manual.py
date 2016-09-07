from kibana import Kibana
from datetime import datetime
import sys

def import_data():
    if len(sys.argv) < 2:
        print "no product"
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
        print "no dates"
        return

    kibana = Kibana()
    for date in dates:
        kibana.import_data(date, product, start_id, recreate_index)


import_data()
