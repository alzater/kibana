import sys
import argparse
import json
from datetime import date, timedelta, datetime
from logger import Logger
from kibana import Kibana


class Configurator:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self._init_parser()
        self._read_config()
        self._parse_params()
        if self._inits():
            print 'Configurating successed'
        else:
            print 'Configurating failed'
            return
        self._set_kibana()
        self._import_data()


    def _init_parser(self):
        self.parser.add_argument('-n', '--names', nargs='+')
        self.parser.add_argument('-d', '--dates', nargs='+')
        self.parser.add_argument('-c', '--catalogues', nargs='+')

        self.parser.add_argument('-r', '--recreate_index', type=bool)
        self.parser.add_argument('-s', '--start_id')
        self.parser.add_argument('-b', '--beg_time')
        self.parser.add_argument('-e', '--end_time')
        self.parser.add_argument('-i', '--id_interval')
        self.parser.add_argument('--source_url')

        self.parser.add_argument('--source_limit_max')
        self.parser.add_argument('--source_limit_min')

        self.parser.add_argument('--elastic_url')
        self.parser.add_argument('--index_prefix')
        self.parser.add_argument('--elastic_type')


    def _parse_params(self):
        params = self.parser.parse_args(sys.argv[1:])
        self.param_data = {}

        self.param_data['names'] = params.names
        self.param_data['dates'] = params.dates
        self.param_data['catalogues'] = params.catalogues

        self.param_data['recreate_index'] = params.recreate_index
        self.param_data['beg_time'] = params.beg_time
        self.param_data['end_time'] = params.end_time
        self.param_data['start_id'] = params.start_id
        self.param_data['id_interval'] = params.id_interval
        self.param_data['source_url'] = params.source_url

        self.param_data['source_limit_max'] = params.source_limit_max
        self.param_data['source_limit_min'] = params.source_limit_min

        self.param_data['elastic_url'] = params.elastic_url
        self.param_data['index_prefix'] = params.index_prefix
        self.param_data['elastic_type'] = params.elastic_type


    def _read_config(self):
        cfg = open('../config/kibana.cfg', 'r')
        self.cfg_data = json.loads(cfg.read())

    def _set_kibana(self):
        self.logger = Logger("manual-"+str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S')))
        self.kibana = Kibana(self.main_params, self.source_params, self.source_limit_params, self.import_params, self.logger.logln)

    def _import_data(self):
        self.kibana.import_data()

    def _inits(self):
        self.main_params = {}
        self.source_params = {}
        self.source_limit_params = {}
        self.import_params = {}

        result = True
        result = result and self._init_names()
        result = result and self._init_dates()
        result = result and self._init_catalogues()
        result = result and self._init_products()

        result = result and self._init_logins()
        result = result and self._init_iter_types()
        
        result = result and self._init_recreate_index()
        result = result and self._init_beg_time()
        result = result and self._init_end_time()
        result = result and self._init_start_id()
        result = result and self._init_id_interval()
        result = result and self._init_source_url()

        result = result and self._init_source_limit_max()
        result = result and self._init_source_limit_min()
        print result
        result = result and self._init_elastic_url()
        result = result and self._init_index_prefix()
        result = result and self._init_elastic_type()
        
        self.source_params['params'] = self.cfg_data['params']
        self.source_params['source_params'] = self.cfg_data['source_params']

        return result


    def _init_names(self):
        if self.param_data['names'] != None:
            self.main_params['names'] = self.param_data['names']
            return True
        if self.cfg_data.get('names') != None:
            self.main_params['names'] = self.cfg_data['names']
            return True
        return False


    def _init_dates(self):
        if self.param_data['dates'] == None:
            yesterday = []
            yesterday.append( str(date.today() - timedelta(1)) )
            self.main_params['dates'] = yesterday
        else:
            self.main_params['dates'] = self.param_data['dates']
        return True


    def _init_catalogues(self):
        if self.param_data['catalogues'] != None:
            default_cat = {}
            default_cat['default'] = self.param_data['catalogues']
            self.main_params['catalogues'] = default_cat
            return True
        if self.cfg_data.get('catalogues') != None:
            self.main_params['catalogues'] = self.cfg_data['catalogues']
            return True
        return False


    def _init_products(self):
        if self.cfg_data.get('products') == None:
            return False
        self.main_params['products'] = self.cfg_data['products']
        return True


    def _init_logins(self):
        if self.cfg_data.get('logins') == None:
            return False
        self.source_params['logins'] = self.cfg_data['logins']
        return True


    def _init_iter_types(self):
        if self.cfg_data.get('iter_types') == None:
            return False
        self.source_params['iter_types'] = self.cfg_data['iter_types']
        return True

    def _init_recreate_index(self):
        if self.param_data.get('recreate_index') != None:
            self.source_params['recreate_index'] = self.param_data['recreate_index']
            return True
        if self.cfg_data.get('recreate_index') != None:
            self.source_params['recreate_index'] = self.cfg_data['recreate_index']
            return True
        return False

    def _init_beg_time(self):
        if self.param_data.get('beg_time') != None:
            self.source_params['beg_time'] = self.param_data['beg_time']
        else:
            self.source_params['beg_time'] = None
        return True

    def _init_end_time(self):
        if self.param_data.get('end_time') != None:
            self.source_params['end_time'] = self.param_data['end_time']
        else:
            self.source_params['beg_time'] = None
        return True

    def _init_start_id(self):
        if self.param_data.get('start_id') != None:
            self.source_params['start_id'] = self.param_data['start_id']
            return True
        if self.cfg_data.get('source_id_start') != None:
            self.source_params['start_id'] = self.cfg_data['source_id_start']
            return True
        return False

    def _init_id_interval(self):
        return True

    def _init_source_url(self):
        if self.param_data.get('source_url') != None:
            self.source_params['source_url'] = self.param_data['source_url']
            return True
        if self.cfg_data.get('source_server') != None:
            self.source_params['source_url'] = self.cfg_data['source_server']
            return True
        return False

    def _init_source_limit_max(self):
        if self.param_data.get('source_limit_max') != None:
            self.source_limit_params['source_limit_max'] = self.param_data['source_limit_max']
            return True
        if self.cfg_data.get('source_limit_max') != None:
            self.source_limit_params['source_limit_max'] = self.cfg_data['source_limit_max']
            return True
        return False

    def _init_source_limit_min(self):
        if self.param_data.get('source_limit_min') != None:
            self.source_limit_params['source_limit_min'] = self.param_data['source_limit_min']
            return True
        if self.cfg_data.get('source_limit_min') != None:
            self.source_limit_params['source_limit_min'] = self.cfg_data['source_limit_min']
            return True
        return False

    def _init_elastic_url(self):
        if self.param_data.get('elastic_url') != None:
            self.import_params['elastic_url'] = self.param_data['elastic_url']
            return True
        if self.cfg_data.get('elastic_url') != None:
            self.import_params['elastic_url'] = self.cfg_data['elastic_url']
            return True
        return False

    def _init_index_prefix(self):
        if self.param_data.get('index_prefix') != None:
            self.import_params['index_prefix'] = self.param_data['index_prefix']
            return True
        if self.cfg_data.get('index_prefix') != None:
            self.import_params['index_prefix'] = self.cfg_data['index_prefix']
            return True
        return False

    def _init_elastic_type(self):
        if self.param_data.get('elastic_type') != None:
            self.import_params['elastic_type'] = self.param_data['elastic_type']
            return True
        if self.cfg_data.get('elastic_type') != None:
            self.import_params['elastic_type'] = self.cfg_data['elastic_type']
            return True
        return False
        
Configurator()
