#import warnings
#warnings.filterwarnings("ignore")

import optparse
import logger
import time
from database import database
from parser import parser
from sniffer import sniffer

class wall:

        def __init__(self):
                self.load_config()

        def run(self):
                self.init_database()
                self.init_parser()
                self.init_sniffer()

        def init_database(self):
                self.database = database(self.options)
                self.database.start()

        def init_parser(self):
                self.parser = parser(self.options, self.database)
                self.parser.start()
                self

        def init_sniffer(self):
                self.sniffer = sniffer(self.options, self.parser, self.database)
                self.sniffer.start()
        
        def load_config(self):
                parser = optparse.OptionParser()
                parser.add_option('-i', '--iface', dest='listen_interface', default='mon0', help='Interface to listen')
                parser.add_option('-p', '--pcap', dest='pcap_file', default='None', help='Pcap file to read')
                parser.add_option('--filter', dest='filter', default='tcp dst port 80 or tcp dst port 8080 or tcp dst port 3128 or tcp dst port 5190 or tcp dst port 110 or tcp dst port 25 or tcp dst port 2041 or tcp dst port 21 or tcp dst port 143', help='Tcpdump filter for password sniff')
                parser.add_option('--p0f-filter', dest='p0f_filter', default='tcp dst port 80 and tcp[tcpflags] & tcp-syn == tcp-syn', help='Tcpdump filter for p0f OS fingerprint')
                parser.add_option('--db', dest='db', default='db.sqlite', help='Sqlite database name')
                parser.add_option('--tcp_timeout', dest='tcp_assemble_timeout', type='int', default='10', help='TCP stream reassemble timeout')

                self.options = parser.parse_args()[0]

if __name__ == "__main__":
        server = wall()
        server.run()
