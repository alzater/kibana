import requests
import json
from source_reader import SourceReader

class Kibana:
    def __init__(self, main_params, source_params, source_limit_params, import_params, log):
        self.log = log
        self.main_params = main_params
        self.source_params = source_params
        self.source_limit_params = source_limit_params
        self.import_params = import_params


    def get_elastic_index( self, date, product_name, catalogue ):
        return self.import_params['elastic_url'] + self.import_params['index_prefix'] + '-' +\
               product_name + '-' + catalogue + '-' + date + '/'


    def get_elastic_index_type( self, date, product_name, catalogue ):
        elastic_type = self.import_params['elastic_type'].get(product_name)
        if elastic_type == None:
            elastic_type = self.import_params['elastic_type']
        return self.get_elastic_index(date, product_name, catalogue) + elastic_type + '/' 

            

    def delete_index(self, date, product_name, catalogue):
        if self.source_params['recreate_index'] == False:
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
        
        elastic_type = self.import_params['elastic_type'].get(product_name)
        if elastic_type == None:
            elastic_type = self.import_params['elastic_type']['default']
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
                            "fuid"      : { "type" : "string", "index" : "not_analyzed" },\
                            "session_id": { "type" : "string", "index" : "not_analyzed" },\
                            "fcookie"   : { "type" : "string", "index" : "not_analyzed" },\
                            "fcatalogue": { "type" : "string", "index" : "not_analyzed" },\
                            "comment"   : { "type" : "string", "index" : "not_analyzed" },\
                            "furlsite"  : { "type" : "string", "index" : "not_analyzed" },\
                            "frefcode"  : { "type" : "string", "index" : "not_analyzed" },\
                            "frefsite1" : { "type" : "string", "index" : "not_analyzed" },\
                            "frefsite2" : { "type" : "string", "index" : "not_analyzed" },\
                            "type"      : { "type" : "string", "index" : "not_analyzed" },\
                            "name"      : { "type" : "string", "index" : "not_analyzed" },\
                            "stat_uuid" : { "type" : "string", "index" : "not_analyzed" },\
                         "event_detail" : { "type" : "string", "index" : "not_analyzed" },\
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


    def import_data(self):


        for date in self.main_params['dates']:
            for product_name in self.main_params['names']:
                catalogues = self.main_params['catalogues'].get(product_name)
                if catalogues == None:
                    catalogues = self.main_params['catalogues']['default']

                for catalogue in catalogues:
                    self.log("NEW FILL! date:"+date+" product_name:"+product_name+" catalogue:"+catalogue)
                    self.recreate_index(date, product_name, catalogue)
                    self.fill_data(date, product_name, catalogue)


    def fill_data(self, date, product_name, catalogue):
        params = self.source_params['params'].get(product_name)
        if params == None:
            params = self.source_params['params']['default']
        
        product = self.main_params['products'].get(product_name)
        if product == None:
            product = product_name
            
        login_data = self.source_params['logins'].get(product_name)
            
        iter_type = self.source_params['iter_types'].get(product_name)
        if iter_type == None:
            iter_type = self.source_params['iter_types']['default']

        for param in params:
            url = self.source_params['source_url'] + '?' + self.source_params['source_params'][param]
            
            reader = SourceReader(date, product, catalogue, url, self.source_params['start_id'], login_data)
            reader.set_beg_time( self.source_params.get('beg_time') )
            reader.set_end_time( self.source_params.get('end_time') )
            reader.set_log(self.log)
            reader.set_limit(self.source_limit_params['source_limit_min'], self.source_limit_params['source_limit_max'])
            reader.set_iter(iter_type, self.source_params['start_id'])
            
            while True:
                result = reader.next_bulk()
                if result == None:
                    break

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

