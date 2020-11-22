import multiprocessing
import os
import argparse
import string
import sys
import time

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


# the asn multiprocessing part
def asn_worker(lock, asn_obj, offset, read_size, flag):
    dao = asn_obj.dao
    counter = 0
    additional_time = (0 if read_size % asn_obj.chunksize == 0 else 1)
    times = read_size // asn_obj.chunksize + additional_time
    last_round_size = read_size - asn_obj.chunksize * (times - additional_time)
    # second partition
    print("process %d start %d round loop, partition offset: %d" % (os.getpid(), times, offset))
    for i in range(times):
        piece_offset = offset + i * asn_obj.chunksize
        limit = asn_obj.chunksize
        if i == times - 1:
            limit = last_round_size
        sql = 'SELECT t.* FROM %s t LIMIT %d OFFSET %d' % \
              (asn_obj.source_name, limit, piece_offset)
        if flag == 0 and i == 0:
            sql = 'SELECT t.* FROM %s t LIMIT %d' % \
                  (asn_obj.source_name, limit)
        lock.acquire()
        df = dao.read_data(sql)
        lock.release()
        update_array = []
        for index, row in df.iterrows():
            ip4_addr = row['ip4_address']
            ip6_addr = row['ip6_address']
            if ip4_addr is not None:
                lookup_result = asn_obj.lookup(ip4_addr)
                if lookup_result is not None:
                    ases = lookup_result[0]
                else:
                    ases = None
            elif ip6_addr is not None:
                lookup_result = asn_obj.lookup(ip6_addr)
                if lookup_result is not None:
                    ases = lookup_result[0]
                else:
                    ases = None
            else:
                ases = None
            update_array.append([ases, piece_offset + index])
        lock.acquire()
        try:
            # directly use update to update the sql table
            update_sql = "UPDATE %s SET ASes = ? WHERE id = ?" % asn_obj.source_name
            dao.conn.executemany(update_sql, update_array)
            dao.conn.commit()
        finally:
            lock.release()
        counter += limit
        sys.stdout.write(
            "\r process %d finished %d... " % (
                os.getpid(), counter))
    print(
        "\r process %d finished %d... " % (
            os.getpid(), counter))


class Asn:

    def __init__(self, db_name, source_name, cache_dir="data", year=2020, month=10, chunksize=1000, process_number=50):
        self.year = year
        self.month = month
        self.link = self.generate_asm_ix_link()
        self.cache_dir = cache_dir
        self.db_name = db_name
        self.source_name = string.capwords(source_name)
        self.chunksize = chunksize
        self.process_number = process_number
        self.dao = Dao.load_table(os.path.join(cache_dir, db_name), self.source_name)
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
        process_number = self.process_number
        self.dao.conn.execute('DROP TABLE IF EXISTS tmp')
        try:
            self.dao.create_column("ASes", "INTEGER")
        except Exception as e:
            print("create fail, maybe exist", str(type(e)))

        all_count = self.dao.conn.execute("SELECT COUNT(*) FROM %s" % self.source_name).fetchone()[0]
        avg_read_number = all_count // process_number

        # begin multiprocessing
        jobs = []
        lock = multiprocessing.Lock()
        start_time = time.time()
        for i in range(process_number):
            offset = avg_read_number * i
            final_read_size = avg_read_number
            if i == process_number - 1:
                final_read_size = avg_read_number + all_count - avg_read_number * process_number
            p = multiprocessing.Process(target=asn_worker,
                                        args=(lock, self, offset, final_read_size, 0 if i == 0 else 1))
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        # after all processes:
        # print the execution time
        print("used time: %f" % (time.time() - start_time))


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
    parser.add_argument('--chunksize', metavar='chunksize', type=int, nargs='?', default=1000,
                        help='the max chunksize to read from sql table every time')
    parser.add_argument('--process', metavar='process', type=int, nargs='?', default=50,
                        help='the number of process to read and update the sql table')
    return parser.parse_args()


if __name__ == '__main__':
    args = init_argparse()
    cache_dir = args.cache
    year = args.year
    month = args.month
    process_number = args.process_number
    chunksize = args.chunksize
    db = args.db
    source = string.capwords(args.source)
    asn = Asn(db, source, cache_dir=cache_dir, year=year, month=month, chunksize=chunksize,
              process_number=process_number)
    asn.flush_ases()
