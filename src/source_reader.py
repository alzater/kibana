import requests
import StringIO
import csv
import urllib
import json
import datetime
import md5

from time import sleep

from limit import Limit
from source_iterator import SourceIterator
from source_row_manager import SourceRowManager

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

        self.beg_time = None
        self.end_time = None

        self.row_manager = SourceRowManager(self.date)


    def set_beg_time(self, time):
        self.beg_time = time


    def set_end_time(self, time):
        self.end_time = time


    def set_log(self, log):
        self.log = log


    def set_limit(self, limit_min, limit_max):
        self.limit = Limit(limit_min, limit_max)


    def set_iter(self, iter_type, index):
        self.iter = SourceIterator(iter_type, index)
        self.iter_type = iter_type
        if self.iter_type == 'HH:MM:SS':
            self.hour = 0


    def next_bulk(self):
        if self.is_last:
            return None

        data, limit = self._get_data()
        if data == None:
            return None

        self.first_row = data.next()

        lines = 0
        fullBulk = False
        while not fullBulk:
            fullBulk = True
            result = []
            #try:
            for cur_row in data:
                lines += 1
                row = self._get_row( cur_row )
                if row == None:
                    self.log("ERROR! Failed to get row.")
                    continue
                if row == 'ignore':
                    continue

                json_row = json.dumps(row)

                result.append(json_row)
            #except:
            #    self.log("EXCEPTION! Failed to get row. id=["+self.iter.get_str()+"]")
            #    if lines != limit:
            #        fullBulk = False

        if self.iter_type == 'HH:MM:SS':
            self.hour += 1

        if lines < 2:
            if not (self.iter_type == 'HH:MM:SS' and self.hour <= 24):
                self.log("FINISH. id=["+self.iter.get_str()+"] limit=["+str(limit)+"]")
                self.is_last = True
        else:
            self.log("Bulk got. id=["+self.iter.get_str()+"] limit=["+str(limit)+"]")

        return result


    def _get_data(self):
        while True:
            url = self._get_url()
            try:
                response = requests.get(url)
            except KeyboardInterrupt:
                self.log("FINISH")
                return None, 0
            except:
                self.log("GET_DATA ERROR! id=[" + self.iter.get_str() + "]")
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

            stream = StringIO.StringIO( response.text.encode('utf-8') )
            return csv.reader( stream, delimiter=',' ), limit


    def _get_row(self, data):
        if len(data) <= 0:
            self.log("ROW ERROR! empty")
            return None

        if self.iter_type == 'id':
            self.iter.set(data[0])
        else:
            self.iter.set(data[1])

        row = self.row_manager.init_row()

        i = 0
        while i < len(data):
            self._add_param(self.first_row[i], data[i], row)
            i += 1
        self.row_manager.apply_rules_to_row(row)

        return row


    def _add_param(self, key, value, obj):
        obj[key] = value


    def _get_url(self):
        params = "date=" + self.date + \
                 self.iter.get_param()+"&p="+self.product+\
                 "&limit=" + str(self.limit.get())+"&pc="+self.catalogue
        if self.beg_time != None:
            params += "&beg_time=" + str(self.beg_time)
        if self.end_time != None:
            params += "&end_time=" + str(self.end_time)

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

        return params


    def _auth(self):
        url = self.url.split('?')[0]
        url += '?method=auth'
        url += '&login=' + self.login_data['login']
        url += '&password=' + self.login_data['password']
        try:
            response = requests.get(url)
        except:
            return False
        return True

