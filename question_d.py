import os
import argparse
import string
import sys

import pandas as pd
from pandao import Dao
from http_query import Downloader
import datetime
import pyasn
from pyasn import mrtx

asm_ix_pattern = "http://archive.routeviews.org/route-views.amsix/bgpdata/%Y.%m/RIBS/rib.%Y%m15.1200.bz2"
asn_filename = 'IPASN.DAT'


# converting MRT/RIB archives to IPASN databases.
def convert(src, dist):
    print('WAIT: converting MRT/RIB archives to IPASN databases.')
    prefixes = mrtx.parse_mrt_file(src, print_progress=True,
                                   skip_record_on_error=True)
    mrtx.dump_prefixes_to_file(prefixes, dist, src)
    v6 = sum(1 for x in prefixes if ':' in x)
    v4 = len(prefixes) - v6
    print('IPASN database saved (%d IPV4 + %d IPV6 prefixes)' % (v4, v6))


class Asn:

    def __init__(self, db_name, source_name, cache_dir="data", year=2020, month=10, chunksize=10000):
        self.year = year
        self.month = month
        self.link = self.generate_asm_ix_link()
        self.cache_dir = cache_dir
        self.db_name = db_name
        self.source_name = string.capwords(source_name)
        self.chunksize = chunksize
        self.dao = Dao.load_table(os.path.join(cache_dir, db_name), source_name)
        self._asndb = self.prepare_databases()

    def generate_asm_ix_link(self):
        return datetime.date(self.year, self.month, 15).strftime(asm_ix_pattern)

    def prepare_databases(self):
        # download
        # todo: Downloader.download_file(self.link, self.cache_dir)
        # convert
        file, path = Downloader.get_file_and_path(self.link, self.cache_dir)
        asn_data_path = os.path.join('data', asn_filename)
        # todo: convert(src=path, dist=asn_data_path)
        # load the asn dat file
        return pyasn.pyasn(asn_data_path)

    def lookup(self, ip_address):
        return self._asndb.lookup(ip_address)

    def flush_ases(self):
        self.dao.conn.execute('DROP TABLE IF EXISTS tmp')
        counter = 0
        data_frames = self.dao.read_data('select * from %s' % self.source_name,
                                         chunksize=self.chunksize)
        for df in data_frames:
            # add empty column
            df["ASes"] = ""
            ases_array = []
            for index, row in df.iterrows():
                ip4_addr = row['ip4_address']
                ip6_addr = row['ip6_address']
                if ip4_addr is not None:
                    if self.lookup(ip4_addr) is not None:
                        ases = self.lookup(ip4_addr)[0]
                    else:
                        ases = None
                elif ip6_addr is not None:
                    if self.lookup(ip6_addr) is not None:
                        ases = self.lookup(ip6_addr)[0]
                    else:
                        ases = None
                else:
                    ases = None
                df.loc[index, 'ASes'] = ases
                ases_array.append(ases)
            new_column = pd.DataFrame({"ASes": ases_array})
            new_column.to_sql('tmp', self.dao.conn, if_exists='append', index_label='id')
            counter += self.chunksize
            sys.stdout.write(
                "\r finished %d... " % (
                    counter))
        self.dao.create_column("ASes", "Integer")
        # merge column
        qry = 'update %s set ASes = (select ASes from tmp where tmp.id = %s.id) ' \
              % (self.source_name, self.source_name)
        self.dao.conn.execute(qry)
        self.dao.conn.commit()
        self.dao.conn.execute('DROP TABLE IF EXISTS tmp')


def init_argparse():
    parser = argparse.ArgumentParser(description='question_d')
    parser.add_argument('source', metavar='source', type=str,
                        help='the source of the data, can be Alexa or Umbrella')
    parser.add_argument('year', metavar='year', type=int,
                        help='the year to query, example: 10')
    parser.add_argument('month', metavar='month', type=int,
                        help='the month to query, example: 2')
    parser.add_argument('--db', metavar='db_name', type=str, nargs='?', default='example.db',
                        help='the db name (name only), example: "example.db"')
    parser.add_argument('--cache', metavar='cache_dir', type=str, nargs='?', default='data',
                        help='the cache folder, example: data')
    parser.add_argument('--chunksize', metavar='chunksize', type=int, nargs='?', default=10000,
                        help='the max chunksize to read and modify from sql table')
    return parser.parse_args()


if __name__ == '__main__':
    args = init_argparse()
    cache_dir = args.cache
    year = args.year
    month = args.month
    chunksize = args.chunksize
    db = args.db
    source = string.capwords(args.source)
    asn = Asn(db, source, cache_dir=cache_dir, year=year, month=month, chunksize=chunksize)
    # too slow
    asn.flush_ases()
