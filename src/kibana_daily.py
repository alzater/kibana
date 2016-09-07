from kibana import Kibana

import json
from datetime import date, timedelta


def _read_config():
    cfg = open('../config/kibana.cfg', 'r')
    cfg_data = json.loads(cfg.read())

    return cfg_data['products']


def kibana_daily():
    yesterday = date.today() - timedelta(1)
    products = _read_config()

    kibana = Kibana()
     
    for product in products:
        kibana.import_data( str(yesterday), product, 0, True )


kibana_daily()

