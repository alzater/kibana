import requests
import StringIO
import csv
import urllib
import json
import urllib
import datetime
from limit import Limit

class SourceReader:
    def __init__(self, date, product, catalogue, url, index):
        self.date = date
        self.product = product
        self.catalogue = catalogue
        self.url = url
        self.index = index

        self.is_last = False


    def set_limit(self, limit_min, limit_max):
        self.limit = Limit(limit_min, limit_max)

    def next_bulk(self):
        if self.is_last:
            return None

        fullBulk = False
        while not fullBulk:
            fullBulk = True

            data, limit = self._get_data()
            if data == None:
                return None

            self.first_row = data.next()
            
            result = []
            #try:
            for cur_row in data:
                row = self._get_row( cur_row )
                if row == None:
                    print "ERROR! Failed to get row."
                    continue
                
                row['date'] = self.date
                row['datetime'] = self._get_datetime(row)

                json_row = json.dumps(row)

                result.append(json_row)
            #except:
            #    print "EXCEPTION! Failed to get row. id:", self.index
            #    if data.line_num != limit + 1:
            #        fullBulk = False
            
        if data.line_num != limit + 1:
	    print "FINISH. id:", self.index, "; limit:", limit
            self.is_last = True

        return result


    def _get_data(self):
        while True:
            url = self.url + "&date=" + self.date + \
                    "&id="+str(self.index)+"&p="+self.product+\
                    "&limit=" + str(self.limit.get())+"&pc="+self.catalogue
            try:
                response = requests.get(url)
            except KeyboardInterrupt:
                print "FINISH"
                return None, 0
            except:
                print "GET_DATA ERROR! id=" + str(self.index)
                self.limit.decrease()
                continue

            if response.status_code != 200:
                print "GET_DATA ERROR! code=" + str(response.status_code) + \
                    " text=" + response.text
                self.limit.decrease()
                continue

            limit = self.limit.get()
            self.limit.increase()
            
            stream = StringIO.StringIO( response.text )
            return csv.reader( stream, delimiter=',' ), limit


    def _get_row(self, data):
        if len(data) <= 0:
            print "ROW ERROR! empty"
            return None

        self.index = int(data[0]) + 1

        row = {}
        i = 0
        while i < len(data):
            if self.first_row[i] == "fvar":
                self._parse_fvar( data[i], row )
            else:
                self._add_param(self.first_row[i], data[i], row)
            i += 1

        return row


    def _get_datetime(self, row):
        date = row['date'].split('-')
        time = row['ftime'].split(':')
        dt = datetime.datetime(int(date[0]), int(date[1]), int(date[2]),\
                               int(time[0]), int(time[1]), int(time[2]) )
        return str(dt)


    def _parse_fvar(self, fvar, row):
        params = fvar.split('&')
        for param in params:
            try:
                key, value = param.split('=')
            except:
                continue
            self._add_param(key, urllib.unquote(value), row)


    def _add_param(self, key, value, obj):
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
        elif key == "fps":
            value = int(value)
        elif key == "exp":
            value = int(value)
        elif key == "avg_frame_time":
            value = int(value)
        elif key == "max_frame_time":
            value = int(value)

        obj[key] = value



