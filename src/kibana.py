import requests
import json
from source_reader import SourceReader

class Kibana:
    def __init__(self, log):
        self.log = log

        if self.read_config() == False:
            self.log("Error! Failed to load config! Exit!")
            return


    def get_elastic_index( self, date, product_name, catalogue ):
        return self.elastic_url + self.index_prefix + '-' +\
               product_name + '-' + catalogue + '-' + date + '/'


    def get_elastic_index_type( self, date, product_name, catalogue ):
        elastic_type = self.elastic_type.get(product_name)
        if elastic_type == None:
            elastic_type = self.elastic_type['default']
        return self.get_elastic_index(date, product_name, catalogue) + elastic_type + '/' 


    def read_config(self):
        cfg = open('../config/kibana_new.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.names = cfg_data.get('names')
        if self.names == None:
            return False
        self.products = cfg_data.get('products')
        if self.products == None:
            return False
        self.catalogues = cfg_data.get('catalogues')
        if self.catalogues == None:
            return False
        self.source_server = cfg_data.get('source_server')
        if self.source_server == None:
            return False
        self.source_params = cfg_data.get('source_params')
        if self.source_params == None:
            return False
        self.params = cfg_data.get('params')
        if self.params == None:
            return False
        self.logins = cfg_data.get('logins')
        if self.logins == None:
            return False
        self.iter_types = cfg_data.get('iter_types')
        if self.iter_types == None:
            return False
        self.source_id_start = cfg_data.get('source_id_start')
        if self.source_id_start == None:
            return False
        self.source_limit_max = cfg_data.get('source_limit_max')
        if self.source_limit_max == None:
            return False
        self.source_limit_min= cfg_data.get('source_limit_min')
        if self.source_limit_min == None:
            return False
        self.elastic_url = cfg_data.get('elastic_url')
        if self.elastic_url == None:
            return False
        self.need_recreate_index = cfg_data.get('recreate_index')
        if self.need_recreate_index == None:
            return False
        self.index_prefix = cfg_data.get('index_prefix')
        if self.index_prefix == None:
            return False
        self.elastic_type = cfg_data.get('elastic_type')
        if self.elastic_type == None:
            return False
        return True

            

    def delete_index(self, date, product_name, catalogue):
        if self.need_recreate_index == False:
            self.log("RECREATE INDEX: false")
            return

        self.log("ELASTIC DELETE INDEX")
        try:
            res = requests.delete(self.get_elastic_index(date, product_name, catalogue))
        except:
            self.log("DELETE INDEX ERROR!")
            return
        self.log(res.text)


    def create_index(self, date, product_name, catalogue):
        self.log("ELASTIC CREATE INDEX")
        
        elastic_type = self.elastic_type.get(product_name)
        if elastic_type == None:
            elastic_type = self.elastic_type['default']
        try:
            res = requests.put(self.get_elastic_index(date, product_name, catalogue), '{  \
                "settings" : {                                                            \
                    "number_of_shards" : 1                                                \
                },                                                                        \
                "mappings" : {                                                            \
                    "'+ elastic_type +'" :{                                               \
                        "properties" : {                                                  \
                            "event"     : { "type" : "string", "index" : "not_analyzed" },\
                            "fproduct"  : { "type" : "string", "index" : "not_analyzed" },\
                            "fcookie"   : { "type" : "string", "index" : "not_analyzed" },\
                            "fcatalogue": { "type" : "string", "index" : "not_analyzed" },\
                            "comment"   : { "type" : "string", "index" : "not_analyzed" },\
                            "furlsite"  : { "type" : "string", "index" : "not_analyzed" },\
                            "frefcode"  : { "type" : "string", "index" : "not_analyzed" },\
                            "frefsite1" : { "type" : "string", "index" : "not_analyzed" },\
                            "frefsite2" : { "type" : "string", "index" : "not_analyzed" },\
                            "type"      : { "type" : "string", "index" : "not_analyzed" },\
                            "name"      : { "type" : "string", "index" : "not_analyzed" },\
                            "datetime"  : { "type" : "date", "format" : "yyyy-MM-dd HH:mm:ss" }\
                        }                                                                 \
                    }                                                                     \
                }                                                                         \
            }')
        except:
            self.log("CREATE INDEX ERROR!")
            return
        self.log(res.text)


    def recreate_index(self, date, product_name, catalogue):
        self.delete_index(date, product_name, catalogue)
        self.create_index(date, product_name, catalogue)


    def import_data(self, date, product_name, source_id=None, recr_index=None):
        if recr_index == None:
            recr_index = self.need_recreate_index
            
        catalogues = self.catalogues.get(product_name)
        if catalogues == None:
            catalogues = self.catalogues['default']

        for catalogue in catalogues:
            self.log("NEW FILL! date:"+date+" product_name:"+product_name+" catalogue:"+catalogue)
            
            if recr_index:
                self.recreate_index(date, product_name, catalogue)

            if source_id != None:
                self.source_id = source_id
            else:
                self.source_id = self.source_id_start

            self.fill_data(date, product_name, catalogue)


    def fill_data(self, date, product_name, catalogue):
        params = self.params.get(product_name)
        if params == None:
            params = self.params['default']
        
        product = self.products.get(product_name)
        if product == None:
            product = product_name
            
        login_data = self.logins.get(product_name)
            
        iter_type = self.iter_types.get(product_name)
        if iter_type == None:
            iter_type = self.iter_types['default']
            
        for param in params:
            url = self.source_server + '?' + self.source_params[param]
            
            reader = SourceReader(date, product, catalogue, url, self.source_id, login_data)
            reader.set_log(self.log)
            reader.set_limit(self.source_limit_min, self.source_limit_max)
            reader.set_iter(iter_type, self.source_id)
            
            while True:
                result = reader.next_bulk()
                if result == None:
                    return

                bulk = ""
                for record in result:
                    bulk += '{"index":{}}\n'
                    bulk += record + '\n'
                
                url = self.get_elastic_index_type(date, product_name, catalogue) + "_bulk"
                try:
                    response = requests.post(url, bulk)
                except KeyboardInterrupt:
                    self.log("STOPPED")
                    return
                except:
                    self.log("FAILED TO INSERT DATA IN KIBANA")
                    continue

