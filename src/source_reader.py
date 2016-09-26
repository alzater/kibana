import requests
import StringIO
import csv
import urllib
import json
import urllib
import datetime
import md5

from limit import Limit
from source_iterator import SourceIterator

class SourceReader:
    def __init__(self, date, product, catalogue, url, index, login_data):
        self.date = date
        self.product = product
        self.catalogue = catalogue
        self.url = url
        self.index = index
        self.login_data = login_data
        
        self.is_last = False
        self.auth = False
        
        
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
                self.log("EXCEPTION! Failed to get row. id=["+self.iter.get_str()+"]")
                if data.line_num != limit + 1:
                    fullBulk = False
            
        if data.line_num != limit + 1:
	        self.log("FINISH. id=["+self.iter.get_str()+"] limit=["+str(limit)+"]")
	        self.is_last = True
        else:
            self.log("Bulk got. id=["+self.iter.get_str()+"] limit=["+str(limit)+"]")

        return result


    def _get_data(self):
        while True:
            url = self._get_url()
            print url
            try:
                response = requests.get(url)
                print response.text
            except KeyboardInterrupt:
                self.log("FINISH")
                return None, 0
            except:
                self.log("GET_DATA ERROR! id=[" + self.iter.get_str()) + "]"
                self.limit.decrease()
                sleep(1)
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

        self.iter.set(data[0])

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
        
        
    def _get_url(self):
        params = "date=" + self.date + \
                 self.iter.get_param()+"&p="+self.product+\
                 "&limit=" + str(self.limit.get())+"&pc="+self.catalogue
        params = self._add_auth(params)
        
        url = self.url + "&" + params
        return url
        
        
    def _add_auth(self, params):
        if self.login_data == None:
            return params
        if self.login_data.get('login') == None:
            return params
        if self.login_data.get('password') == None:
            return params
            
        if self.auth == False:
            if self._auth() != True:
                return params

        params += '&login=' + self.login_data['login']

        data = params.split('&')
        data.sort()
        data = ''.join(data)
        data += self.login_data['password']
        
        sign = md5.new(data).hexdigest()
        params += '&sign=' + sign
        print sign
        
        return params
        
    
    def _auth(self):
        url = self.url.split('?')[0]
        url += '?method=auth'
        url += '&login=' + self.login_data['login']
        url += '&password=' + self.login_data['password']
        
        try:
            print url
            response = requests.get(url)
            print response.text
        except:
            return False
        
        return True
        

