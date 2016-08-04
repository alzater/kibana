import requests
import StringIO
import csv
import json
import urllib

class Kibana:
    def __init__(self):
        self.read_config()

        self.source_id = 0
        self.source_limit = 10000

        self.is_first = True

        self.elastic_index = self.elastic_url + 'test-' + self.date + '/'
        self.elastic_index_type = self.elastic_index + self.source_product + '/'

        self.debug_break = False


    def read_config(self):
        cfg = open('kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.source_url = cfg_data['source_url']
        self.source_product = cfg_data['source_product']
        self.elastic_url = cfg_data['elastic_url']
        self.date = cfg_data['date']


    def recreate_index(self):
        print "ELASTIC DELETE INDEX"
        try:
            res = requests.delete(self.elastic_index)
        except:
            print "DELETE INDEX ERROR!"
            return
        print res.text

        print "ELASTIC CREATE INDEX"
        try:
            requests.put(self.elastic_index, '{                                         \
                "settings" : {                                                          \
                    "number_of_shards" : 1                                              \
                },                                                                      \
                "mappings" : {                                                          \
                    '+ self.source_product +' : {                                       \
                        "properties" : {                                                \
                            "comment" : { "type" : "string", "index" : "not_analyzed" } \
                            "type"    : { "type" : "string", "index" : "not_analyzed" } \
                        }                                                               \
                    },                                                                  \
                }                                                                       \
            }')
        except:
            print "CREATE INDEX ERROR!"
            return
        print res.text


    def get_source_data(self):
        source_url = self.source_url + "&date=" + self.date + \
                    "&id="+str(self.source_id)+"&p="+self.source_product+"&limit=" + str(self.source_limit)

        try:
            response = requests.get( source_url )
        except:
            print "SOURCE ERROR! id=", self.source_id
            return None

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
        elif key == "frefcode":
            return
        elif key == "frefsite1":
            return
        elif key == "frefsite2":
            return
        elif key == "furi":
            return
        elif key == "furlsite":
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


    def fill_data(self):
        self.recreate_index()

        while True:
            result = ""

            source_data = self.get_source_data()
            if source_data == None:
                return

            self.first_row = source_data.next()

            for source_row in source_data:
                row = self.get_row( source_row )
                if row == None:
                    return

                json_row = json.dumps(row)

                result += '{"index":{}}\n'
                result += json_row + '\n'

            url = self.elastic_index_type + "_bulk"
            try:
                response = requests.post( url, result)
            except:
                print "FAILED TO INSERT DATA IN KIBANA"

            if self.debug_break or source_data.line_num != self.source_limit + 1:
                break

            print "No break: ", self.source_id


kibana = Kibana()
kibana.fill_data()
