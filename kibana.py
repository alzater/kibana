import requests
import StringIO
import csv
import json
import urllib

class Kibana:
    def __init__(self):
        self.read_config()

        self.date = "2016-08-01"

        self.source_id = 0
        self.source_limit = 9000

        self.is_first = True

        self.elastic_url = self.elastic_index + '/getlog/'

        self.debug_break = False



    def read_config(self):
        cfg = open('kibana.cfg', 'r')
        cfg_data = json.loads(cfg.read())

        self.source_url = cfg_data['source_url']
        self.source_product = cfg_data['source_product']
        self.elastic_index = cfg_data['elastic_index']


    def get_source_data(self):
        source_url = self.source_url + "&date=" + self.date + \
                    "&id="+str(self.source_id)+"&p="+self.source_product+"&limit=" + str(self.source_limit)
        print source_url

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
            key, value = param.split('=')
            row[key] = urllib.unquote(value)

    def get_row(self, source_data):
        source_row = source_data.next()

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
                row[self.first_row[i]] = source_row[i]
            i += 1

        return row


    def create_index(self):
        res = requests.delete( self.elastic_url)
        print res

    def fill_data(self):
        while True:
            result = ""

            source_data = self.get_source_data()
            if source_data == None:
                return False

            self.first_row = source_data.next()

            for source_row in source_data:
                row = self.get_row( source_data )
                if row == None:
                    return
                print "ROW", self.source_id

                json_row = json.dumps(row)

                result += '{"index":{}}\n'
                result += json_row + '\n'

            url = self.elastic_url+ "_bulk"
            print url
            ok = requests.post( url, result)

            if self.debug_break or source_data.line_num != self.source_limit + 1:
                break

            print "No break: ", self.source_id


kibana = Kibana()
kibana.fill_data()
#print requests.delete(kibana.elastic_index);
#print requests.put( kibana.elastic_index, '{\
#    "settings" : {                                           \
#        "number_of_shards" : 1                               \
#    },                                                       \
#    "mappings" : {                                           \
#        "getlog" : {                                          \
#            "properties" : {                                 \
#                "comment" : { "type" : "string", "index" : "not_analyzed" }\
#            }                                                \
#        }                                                    \
#    }                                                        \
#}').text
