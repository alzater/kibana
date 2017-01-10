import urllib
import datetime

class SourceRowManager:
    def __init__(self, date):
        self.date = date


    def init_row(self):
        row = {}
        row['name'] = ""
        row['comment'] = ""
        row['event_detail'] = ""
        return row


    def apply_rules_to_row(self, row):
        self._parse_fvar_rule(row)
        self._datetime_rule(row)

        self._parse_event_rule(row)
        self._parse_event_detail_rule(row)
        self._parse_uid_rule(row)
        self._delete_unused_params_rule(row)
        self._rename_keys_rule(row)

        self._count_profit_rule(row)
        self._remove_test_users_rule(row)
        self._parse_amount_rule(row)


    def _parse_fvar_rule(self, row):
        if row.get('fvar') == None:
            return

        params = row['fvar'].split('&')
        for param in params:
            try:
                key, value = param.split('=')
            except:
                continue
            row[key] = urllib.unquote(value)

        del row['fvar']


    def _parse_event_rule(self, row):
        if row.get('fevent'):
            row['event'] = row['fevent']
            del row['fevent']
        elif row.get('e'):
            row['event'] = row['e']
            del row['e']


    def _parse_event_detail_rule(self, row):
        if row.get("ed"):
            row['event_detail'] = row['ed']
            del row['ed']
        elif row.get("fevent_detail"):
            row['event_detail'] = row['fevent_detail']
            del row['fevent_detail']


    def _parse_uid_rule(self, row):
        if row.get('fid'):
            row['fid'] = int(row['fid'])


    def _delete_unused_params_rule(self, row):
        if row.get('skip'):
            del row['skip']
        if row.get('fgamecode'):
            del row['fgamecode']
        if row.get('fgametype'):
            del row['fgametype']
        if row.get('furi'):
            del row['furi']
        if row.get('ts'):
            del row['ts']


    def _rename_keys_rule(self, row):
        if row.get('os'):
            row['fos'] = row['os']
            del row['os']


    def _count_profit_rule(self, row):
        if row['event'] == 'deposit':
            if row['fproduct'] == 'glads2_vk':
                row['profit'] = float(row['famount']) * 2.91
            if row['fproduct'] == 'glads2_ok':
                row['profit'] = float(row['famount']) * 0.42
            if row['fproduct'] == 'glads2_mm':
                row['profit'] = float(row['famount']) * 0.42


    def _remove_test_users_rule(self, row):
        if row['fuid'] == 232946 or \
           row['fuid'] == 257158814382854806 or \
           row['fuid'] == 7255913029939607050 or \
           row['fuid'] == 5819770344824394920 or \
           row['fuid'] == 72213 or \
           row['fuid'] == 32256090 or \
           row['fuid'] == 14926854930002548513:
            row = 'ignore'
            print "USER_REMOVED"


    def _parse_amount_rule(self, row):
        if row.get('amount'):
            if row.get('event') == 'deposit' or \
               (row.get('event') == 'realin' and row.get('event_detail') == 'first') or \
               (row.get('event') == 'realin' and row.get('event_detail') == 'product'):
                self._convert_value_to_float(row, 'amount')
            else:
                self._convert_value_to_int(row, 'amount')


    def _convert_values_rule(self, row):
        if row.get('famountusd'):
            row['famountusd'] = float(row['famountusd']) / 100

        self._convert_value_to_float(row, 'fps')
        self._convert_value_to_float(row, 'price')

        self._convert_value_to_int(row, 'credits')
        self._convert_value_to_int(row, 'level')
        self._convert_value_to_int(row, 'rubies')
        self._convert_value_to_int(row, 'gold')
        self._convert_value_to_int(row, 'food')
        self._convert_value_to_int(row, 'exp')
        self._convert_value_to_int(row, 'avg_frame_time')
        self._convert_value_to_int(row, 'max_frame_time')
        self._convert_value_to_int(row, 'amount_free')
        self._convert_value_to_int(row, 'amount_pay')

            
    def _datetime_rule(self, row):
        row['date'] = self.date
        date = self.date.split('-')
        time = row['ftime'].split(':')
        dt = datetime.datetime(int(date[0]), int(date[1]), int(date[2]),\
                               int(time[0]), int(time[1]), int(time[2]) )
        row['datetime'] = str(dt)

    
    def _convert_value_to_int(self, row, key):
        if row.get(key):
            row[key] = int(row[key])


    def _convert_value_to_float(self, row, key):
        if row.get(key):
            row[key] = float(row[key])


