import multiprocessing
import os
import argparse
import string
import sys
import time
import re
import pandas as pd
from pandao import Dao
from http_query import Downloader
import datetime


# the host multiprocessing part
def host_worker(lock, host_obj, offset, read_size, flag):
    dao = host_obj.dao
    counter = 0
    additional_time = (0 if read_size % host_obj.chunksize == 0 else 1)
    times = read_size // host_obj.chunksize + additional_time
    last_round_size = read_size - host_obj.chunksize * (times - additional_time)
    # second partition
    print("process %d start %d round loop, partition offset: %d" % (os.getpid(), times, offset))
    for i in range(times):
        piece_offset = offset + i * host_obj.chunksize
        limit = host_obj.chunksize
        if i == times - 1:
            limit = last_round_size
        sql = 'SELECT t.* FROM %s t LIMIT %d OFFSET %d' % \
              (host_obj.source_name, limit, piece_offset)
        if flag == 0 and i == 0:
            sql = 'SELECT t.* FROM %s t LIMIT %d' % \
                  (host_obj.source_name, limit)
        lock.acquire()
        df = dao.read_data(sql)
        lock.release()
        update_array = []
        for index, row in df.iterrows():
            response_type = row['response_type']
            if response_type == 'CNAME':
                hostname = host_obj.identify_cname(row['response_name'], row['query_name'])
            else:
                hostname = host_obj.identify_ases(row['ASes'])
            if hostname is None:
                hostname = "-1"
            update_array.append([hostname, piece_offset + index + 1])
        lock.acquire()
        try:
            # directly use update to update the sql table
            update_sql = "UPDATE %s SET host = ? WHERE ROWID = ?" % host_obj.source_name
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


class Host:
    def __init__(self, db_name, source_name, cache_dir="data", chunksize=1000, process_number=50):
        self.cache_dir = cache_dir
        self.db_name = db_name
        self.source_name = string.capwords(source_name)
        self.chunksize = chunksize
        self.process_number = process_number
        self.dao = Dao(os.path.join(cache_dir, db_name), self.source_name, 0)
        self._init_re_rules()
        self._init_asns_rules()

    def _init_re_rules(self):
        with open(os.path.join('csv', 'regexes.csv')) as f:
            content = f.readlines()
        re_list = []
        for c in content:
            exec("re_list.append(" + c[:len(c) - 1] + ')')
        self.re_list = re_list

    def _init_asns_rules(self):
        df = pd.read_csv(os.path.join('csv', 'asns.csv'), sep=";")
        self.asns = df[['provider', 'asn']].drop_duplicates()

    def identify_ases(self, ases):
        search_df = self.asns.loc[self.asns['asn'] == ases]
        if search_df.empty:
            return None
        else:
            return search_df['provider'].values[0]

    def identify_cname(self, response_name, query_name):
        if response_name != query_name:
            for i in range(len(self.re_list)):
                if self.re_list[i][1].match(response_name) is not None:
                    return self.re_list[i][0]
                if self.re_list[i][1].match(query_name) is not None:
                    return self.re_list[i][0]
        else:
            for i in range(len(self.re_list)):
                if self.re_list[i][1].match(response_name) is not None:
                    return self.re_list[i][0]
        return None

    def flush_host(self):
        process_number = self.process_number
        try:
            self.dao.create_column("host", "TEXT")
        except Exception as e:
            print("create fail, maybe exist", str(type(e)))
        # get the number of rows in the table
        all_count = self.dao.conn.execute("SELECT COUNT(*) FROM %s" % self.source_name).fetchone()[0]
        # average the number of rows that one process(thread) should read
        avg_read_number = all_count // process_number

        # begin multiprocessing
        jobs = []
        lock = multiprocessing.Lock()
        start_time = time.time()
        for i in range(process_number):
            # the offset that this process should start from.
            offset = avg_read_number * i
            # calculate the corner case of the number of rows that one process(thread) should read
            # if the process is the last process, it should handle all of the left rows.
            final_read_size = avg_read_number
            if i == process_number - 1:
                final_read_size = avg_read_number + all_count - avg_read_number * process_number
            # start multi processes
            p = multiprocessing.Process(target=host_worker,
                                        args=(lock, self, offset, final_read_size, 0 if i == 0 else 1))
            jobs.append(p)
            p.start()
        # join to wait until all processes finished.
        for p in jobs:
            p.join()
        # after all processes:
        # print the execution time
        print("used time: %f" % (time.time() - start_time))


def init_argparse():
    parser = argparse.ArgumentParser(description='question_e')
    parser.add_argument('source', metavar='source', type=str,
                        help='the source of the data, can be Alexa or Umbrella')
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
    arg_cache_dir = args.cache
    arg_process_number = args.process_number
    arg_chunksize = args.chunksize
    arg_db = args.db
    arg_source = string.capwords(args.source)
    h = Host(arg_db, arg_source, cache_dir=arg_cache_dir, chunksize=arg_chunksize,
             process_number=arg_process_number)
    h.flush_host()
