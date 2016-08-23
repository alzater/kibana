import requests
import StringIO
import csv
import json
import urllib
import sys
import datetime

class Kibana:
    def __init__(self):
        self.dates = []
        self.source_products = []
        self.source_start_id = 0

        self.read_config()

        self.source_limit = 10000
        self.debug_break = False


    def get_elastic_index( self, date ):
        return self.elastic_url + self.index_prefix + '-' + date + '/'


    def get_elastic_index_type( self, date, product ):
        return self.get_elastic_index(date) + product + '/' 


    def read_config(self):
        cfg = open('kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.source_url = cfg_data['source_url']
        self.elastic_url = cfg_data['elastic_url']
        self.index_prefix = cfg_data['index_prefix']
        self.need_recreate_index = cfg_data['recreate_index']
        self.source_start_id = cfg_data['start_source_id']

	if cfg_data.get('dates'):
            for date in cfg_data['dates']:
            	self.dates.append( date )
        elif cfg_data.get('date'):
            self.dates.append( cfg_data['date'] )

        
	if cfg_data.get('source_products'):
            for product in cfg_data['source_products']:
            	self.source_products.append( product )
        elif cfg_data.get('source_product'):

            self.source_products.append( cfg_data['source_product'] )
            

    def delete_index(self, date):
        if self.need_recreate_index == False:
            print "RECREATE INDEX: false"
            return

        print "ELASTIC DELETE INDEX"
        try:
            res = requests.delete(self.get_elastic_index(date))
        except:
            print "DELETE INDEX ERROR!"
            return
        print res.text


    def create_index(self, date):
        print "ELASTIC CREATE INDEX"
        try:
            res = requests.put(self.get_elastic_index(date), '{                           \
                "settings" : {                                                            \
                    "number_of_shards" : 1                                                \
                },                                                                        \
                "mappings" : {                                                            \
                    "dmplog"      : {                                                     \
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
                            "type"      : { "type" : "string", "index" : "not_analyzed" } \
                        }                                                                 \
                    }                                                                     \
                }                                                                         \
            }')
        except:
            print "CREATE INDEX ERROR!"
            return
        print res.text


    def recreate_index(self, date):
        self.delete_index(date)
        self.create_index(date)


    def get_source_data(self, date, product):
        source_url = self.source_url + "&date=" + date + \
                    "&id="+str(self.source_id)+"&p="+product+"&limit=" + str(self.source_limit)
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


    def get_utime(self, row):
        date = row['date'].split('-')
        time = row['ftime'].split(':')
        dt = datetime.datetime(int(date[0]), int(date[1]), int(date[2]),\
                               int(time[0]), int(time[1]), int(time[2]) )
        return ( dt - datetime.datetime(1970, 1, 1)).total_seconds()
        

    def import_data(self):
        for date in self.dates:
            if self.need_recreate_index:
                self.recreate_index(date)

            for product in self.source_products:
                print "NEW FILL! date=", date, " product=", product

                self.source_id = self.source_start_id

                self.fill_data(date, product)


    def fill_data(self, date, product):
        while True:
            result = ""
            source_data = self.get_source_data(date, product)
            if source_data == "FINISH":
                print "STOPPED BY CLIENT"
                return
            elif source_data == "ERROR":
                print "Try again! id:", self.source_id
                continue

            self.first_row = source_data.next()

            for source_row in source_data:
                row = self.get_row( source_row )
                if row == None:
                    print "ERROR! Failed to get row."
                    continue
                
                row['date'] = date
                row['utime'] = self.get_utime(row)

                json_row = json.dumps(row)

                result += '{"index":{}}\n'
                result += json_row + '\n'

            url = self.get_elastic_index_type(date, product) + "_bulk"
            try:
                response = requests.post(url, result)
            except KeyboardInterrupt:
                print "STOPPED BY CLIENT"
                return
            except:
                print "FAILED TO INSERT DATA IN KIBANA"
                continue

            if self.debug_break or source_data.line_num != self.source_limit + 1:
		print "FINISH. id: ", self.source_id
                break

            print "NEXT BULK. Current id: "+str(self.source_id)


kibana = Kibana()
kibana.import_data()
