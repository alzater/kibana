import requests
import StringIO
import csv
import json
import urllib
import sys
import datetime
from limit import Limit

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

        self.limit = Limit(self.source_limit_min, self.source_limit_max)



    def get_elastic_index( self, date, product, catalogue ):
        return self.elastic_url + self.index_prefix + '-' +\
               product + '-' + catalogue + '-' + date + '/'


    def get_elastic_index_type( self, date, product, catalogue ):
        return self.get_elastic_index(date, product, catalogue) + self.elastic_type + '/' 


    def read_config(self):
        cfg = open('kibana.cfg', 'r')
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


    def get_source_data(self, date, product, catalogue):
        source_url = self.source_url + "&date=" + date + \
                    "&id="+str(self.source_id)+"&p="+product+\
                    "&limit=" + str(self.limit.get())+"&pc="+catalogue
        try:
            response = requests.get( source_url )
        except KeyboardInterrupt:
            print "FINISH"
            return "FINISH"
        except:
            print "GET_SOURCE_DATA ERROR! id=" + str(self.source_id)
            return "ERROR"

        if response.status_code != 200:
            print "GET_SOURCE_DATA ERROR! code=" + str(response.status_code) + \
                " text=" + response.text
            return "ERROR"
 
        stream = StringIO.StringIO( response.text )
        return csv.reader( stream, delimiter=',' )


    def parse_fvar(self, fvar, row):
        params = fvar.split('&')
        for param in params:
            try:
                key, value = param.split('=')
            except:
                continue
            self.set_param_to_object(key, urllib.unquote(value), row)


    def set_param_to_object(self, key, value, obj):
        if key == "fevent":
            key = "event"
        elif key == "e":
            key = "event"
        elif key == "skip":
            return
        elif key == "fgamecode":
            return
        elif key == "fgametype":
            return
        elif key == "furi":
            return
        elif key == "os":
            key = "fos"
        elif key == "ts":
            return
        elif key == "amount":
            value = int(value)
        elif key == "rubies":
            value = int(value)
        elif key == "gold":
            value = int(value)
        elif key == "food":
            value = int(value)
        #elif key == "fps":
        #    value = int(value)
        elif key == "exp":
            value = int(value)
        elif key == "avg_frame_time":
            value = int(value)
        elif key == "max_frame_time":
            value = int(value)

        obj[key] = value


    def get_row(self, source_row):
        if len(source_row) <= 0:
            print "SOURCE ROW ERROR! empty"
            return None

        self.source_id = int(source_row[0]) + 1

        row = {}
        i = 0
        while i < len(source_row):
            if self.first_row[i] == "fvar":
                self.parse_fvar( source_row[i], row )
            else:
                self.set_param_to_object(self.first_row[i], source_row[i], row)
            i += 1

        return row


    def get_datetime(self, row):
        date = row['date'].split('-')
        time = row['ftime'].split(':')
        dt = datetime.datetime(int(date[0]), int(date[1]), int(date[2]),\
                               int(time[0]), int(time[1]), int(time[2]) )
        return str(dt)

    def import_data(self, date, product, source_id=None, recr_index=None):
        if recr_index == None:
            recr_index = self.need_recreate_index

        if source_id != None:
            self.source_id = source_id
        else:
            self.source_id = self.source_id_start

        if self.catalogues.get(product):
            catalogues = self.catalogues[product]
        else:
            catalogues = self.catalogues['default']

        for catalogue in catalogues:
            print "NEW FILL! date:", date, "product:", product, "catalogue:", catalogue
            if recr_index:
                self.recreate_index(date, product, catalogue)

            self.source_id = self.source_id_start
            self.fill_data(date, product, catalogue)

    def fill_data(self, date, product, catalogue):
        while True:
            result = ""
            limit = self.limit.get()
            source_data = self.get_source_data(date, product, catalogue)

            if source_data == "FINISH":
                print "STOPPED"
                return
            elif source_data == "ERROR":
                print "Try again! id:", self.source_id
                self.limit.decrease()
                continue

            self.limit.increase()

            self.first_row = source_data.next()

            for source_row in source_data:
                row = self.get_row( source_row )
                if row == None:
                    print "ERROR! Failed to get row."
                    continue
                
                row['date'] = date
                row['datetime'] = self.get_datetime(row)

                json_row = json.dumps(row)

                result += '{"index":{}}\n'
                result += json_row + '\n'

            url = self.get_elastic_index_type(date, product, catalogue) + "_bulk"
            try:
                response = requests.post(url, result)
            except KeyboardInterrupt:
                print "STOPPED"
                return
            except:
                print "FAILED TO INSERT DATA IN KIBANA"
                continue

            if source_data.line_num != limit + 1:
		print "FINISH. id:", self.source_id, "; limit:", limit
                break

            print "NEXT BULK. Current id:", str(self.source_id), "; limit:", limit

