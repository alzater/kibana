import requests
import StringIO
import csv
import urllib
import json
import urllib
import datetime

from limit import Limit
from source_iterator import SourceIterator

class SourceReader:
    def __init__(self, date, product, catalogue, url, index):
        self.date = date
        self.product = product
        self.catalogue = catalogue
        self.url = url
        self.index = index

        self.is_last = False
        
        
    def set_log(self, log):
        self.log = log


    def set_limit(self, limit_min, limit_max):
        self.limit = Limit(limit_min, limit_max)
        
        
    def set_iter(self, iter_type, index):
        self.iter = SourceIterator(iter_type, index)


    def next_bulk(self):
        if self.is_last:
            return None

        data, limit = self._get_data()
        if data == None:
            return None

        self.first_row = data.next()    
   
        fullBulk = False
        while not fullBulk:
            fullBulk = True           
            result = []
            try:
                for cur_row in data:
                    row = self._get_row( cur_row )
                    if row == None:
                        self.log("ERROR! Failed to get row.")
                        continue
                    
                    row['date'] = self.date
                    row['datetime'] = self._get_datetime(row)

                    json_row = json.dumps(row)

                    result.append(json_row)
            except:
                self.log("EXCEPTION! Failed to get row. id=["+str(self.iter.get())+"]")
                if data.line_num != limit + 1:
                    fullBulk = False
            
        if data.line_num != limit + 1:
	        self.log("FINISH. id=["+str(self.iter.get())+"] limit=["+str(limit)+"]")
	        self.is_last = True
        else:
            self.log("Bulk got. id=["+str(self.iter.get())+"] limit=["+str(limit)+"]")

        return result


    def _get_data(self):
        while True:
            url = self.url + "&date=" + self.date + \
                    +self.iter.getStr()+"&p="+self.product+\
                    "&limit=" + str(self.limit.get())+"&pc="+self.catalogue
            try:
                response = requests.get(url)
            except KeyboardInterrupt:
                self.log("FINISH")
                return None, 0
            except:
                self.log("GET_DATA ERROR! id=" + str(self.iter.get()))
                self.limit.decrease()
                continue

            if response.status_code != 200:
                self.log( "GET_DATA ERROR! code=" + str(response.status_code) + \
                    " text=" + response.text)
                self.limit.decrease()
                continue

            limit = self.limit.get()
            self.limit.increase()
            
            stream = StringIO.StringIO( response.text )
            return csv.reader( stream, delimiter=',' ), limit


    def _get_row(self, data):
        if len(data) <= 0:
            self.log("ROW ERROR! empty")
            return None

        self.iter.set(int(data[0]))

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
        elif key == "credits":
            value = int(value)
        elif key == "level":
            value = int(value)
        elif key == "rubies":
            value = int(value)
        elif key == "gold":
            value = int(value)
        elif key == "food":
            value = int(value)
        elif key == "fps":
            value = float(value)
        elif key == "exp":
            value = int(value)
        elif key == "avg_frame_time":
            value = int(value)
        elif key == "max_frame_time":
            value = int(value)

        obj[key] = value



