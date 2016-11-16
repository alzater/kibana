import sys
import argparse
 
class Configuretor:
    def __init__(self, args):
        self.parser = argparse.ArgumentParser()
        self.init_parser()


    def _init_parser(self):
        self.parser.add_argument('-n', '--names', nargs='+')
        self.parser.add_argument('-r', '--recreate_index', type=bool)
        self.parser.add_argument('-d', '--dates', nargs='+')
        self.parser.add_argument('-c', '--catalogues', nargs='+')

        self.parser.add_argument('-s', '--start_id')       
        self.parser.add_argument('-b', '--beg_time')
        self.parser.add_argument('-e', '--end_time')
        self.parser.add_argument('-i', '--id_interval')
        self.parser.add_argument('--source_url')
        self.parser.add_argument('source_limit_max')
        self.parser.add_argument('source_limit_min')

        self.parser.add_argument('elastic_url')
        self.parser.add_argument('recreate_index')
        self.parser.add_argument('index_prefix')
        self.parser.add_argument('elastic_type')


    def _read_config(self):
        print


    def _print_help(self):
        print
        
 
#  example
#if __name__ == '__main__':
#    parser = createParser()
#    namespace = parser.parse_args(sys.argv[1:])
# 
#    print (namespace)
# 
#    for _ in range (namespace.count):
#        print ("Hello, {}!".format (namespace.name) )
