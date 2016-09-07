import requests
import json
from source_reader import SourceReader

class Kibana:
    def __init__(self):
        self.products = []
        self.dates = []
        self.catalogues = {}

        self.source_id_start = 0
        self.source_limit_max = 10000
        self.source_limit_min = 10

        self.elastic_type = "log"

        self.read_config()


    def get_elastic_index( self, date, product, catalogue ):
        return self.elastic_url + self.index_prefix + '-' +\
               product + '-' + catalogue + '-' + date + '/'


    def get_elastic_index_type( self, date, product, catalogue ):
        return self.get_elastic_index(date, product, catalogue) + self.elastic_type + '/' 


    def read_config(self):
        cfg = open('../config/kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.source_url = cfg_data['source_url']
        self.elastic_url = cfg_data['elastic_url']
        self.elastic_type = cfg_data['elastic_type']
        self.index_prefix = cfg_data['index_prefix']
        self.need_recreate_index = cfg_data['recreate_index']
        self.source_id_start = cfg_data['source_id_start']
        self.source_limit_max = cfg_data['source_limit_max']
        self.source_limit_min = cfg_data['source_limit_min']
        self.catalogues = cfg_data['catalogues']
            

    def delete_index(self, date, product, catalogue):
        if self.need_recreate_index == False:
            print "RECREATE INDEX: false"
            return

        print "ELASTIC DELETE INDEX"
        try:
            res = requests.delete(self.get_elastic_index(date, product, catalogue))
        except:
            print "DELETE INDEX ERROR!"
            return
        print res.text


    def create_index(self, date, product, catalogue):
        print "ELASTIC CREATE INDEX"

        try:
            res = requests.put(self.get_elastic_index(date, product, catalogue), '{       \
                "settings" : {                                                            \
                    "number_of_shards" : 1                                                \
                },                                                                        \
                "mappings" : {                                                            \
                    "'+ self.elastic_type +'" :{                                          \
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
                            "datetime"  : { "type" : "date", "format" : "yyyy-MM-dd HH:mm:ss" }\
                        }                                                                 \
                    }                                                                     \
                }                                                                         \
            }')
        except:
            print "CREATE INDEX ERROR!"
            return
        print res.text


    def recreate_index(self, date, product, catalogue):
        self.delete_index(date, product, catalogue)
        self.create_index(date, product, catalogue)


    def import_data(self, date, product, source_id=None, recr_index=None):
        if recr_index == None:
            recr_index = self.need_recreate_index

        if self.catalogues.get(product):
            catalogues = self.catalogues[product]
        else:
            catalogues = self.catalogues['default']

        for catalogue in catalogues:
            print "NEW FILL! date:", date, "product:", product, "catalogue:", catalogue
            if recr_index:
                self.recreate_index(date, product, catalogue)

            if source_id != None:
                self.source_id = source_id
            else:
                self.source_id = self.source_id_start

            self.fill_data(date, product, catalogue)


    def fill_data(self, date, product, catalogue):
        reader = SourceReader(date, product, catalogue, self.source_url, self.source_id)
        reader.set_limit(self.source_limit_min, self.source_limit_max)

        while True:
            result = reader.next_bulk()
            if result == None:
                return

            bulk = ""
            for record in result:
                bulk += '{"index":{}}\n'
                bulk += record + '\n'
            
            url = self.get_elastic_index_type(date, product, catalogue) + "_bulk"
            try:
                response = requests.post(url, bulk)
            except KeyboardInterrupt:
                print "STOPPED"
                return
            except:
                print "FAILED TO INSERT DATA IN KIBANA"
                continue

