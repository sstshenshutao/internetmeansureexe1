import multiprocessing
import os
import sys
import argparse
import string

from pandao import Dao
from http_query import Downloader
from fastavro import reader as avro_reader
import tarfile


def filter_record(record):
    return str(record['query_name']).startswith('www') and (
            record['response_type'] == 'AAAA' or record['response_type'] == 'A' or
            record['response_type'] == 'CNAME')


# the worker for the avro file, each worker works for a single avro file that is extracted from the tar
def avro_worker(lck, tar_file_name):
    with worker_number.get_lock():
        worker_number.value += 1
    with open(tar_file_name, 'rb') as fo:
        avro_io = avro_reader(fo)
        counter = 0
        if buffer_max == 0:
            sys.stdout.write(
                "\r extracting %s... [worker number: %d]" % (
                    tar_file_name, worker_number.value))
            all_records = [record for record in avro_io if filter_record(record)]
            counter += len(all_records)
            dao.insert_all_records(all_records, lck)
        else:
            for record in avro_io:
                if filter_record(record):
                    if counter % buffer_max == 0:
                        if counter != 0:
                            with all_counter.get_lock():
                                all_counter.value += buffer_max
                        sys.stdout.write(
                            "\r extracting %s: %d... [worker number: %d]" % (
                                tar_file_name, all_counter.value, worker_number.value))
                        sys.stdout.flush()
                    dao.insert_data(record, lck)
                    counter += 1
            dao.flush(lck)
            with all_counter.get_lock():
                all_counter.value += (counter % buffer_max)
        print(
            "\r extracting %s: %d...ok " % (tar_file_name, counter))
    with worker_number.get_lock():
        worker_number.value -= 1
    # clean data file
    os.remove(tar_file_name)


def init_argparse():
    parser = argparse.ArgumentParser(description='question_b')
    parser.add_argument('source', metavar='source', type=str,
                        help='the source of the data, can be Alexa or Umbrella')
    parser.add_argument('year', metavar='year', type=int,
                        help='the year to query, example: 10')
    parser.add_argument('month', metavar='month', type=int,
                        help='the month to query, example: 2')
    parser.add_argument('--db', metavar= 'db_name', type=str, nargs='?', default='example.db',
                        help='the db name (name only), example: "example.db"')
    parser.add_argument('--cache', metavar='cache_dir', type=str, nargs='?', default='data',
                        help='the cache folder, example: data')
    parser.add_argument('--buffer', metavar='buffer_max', type=int, nargs='?', default=0,
                        help='the max buffer size: the size of avro entries that are buffered in the memory')
    return parser.parse_args()


if __name__ == '__main__':
    args = init_argparse()
    source_name = string.capwords(args.source)
    cache_dir = args.cache
    db_name = args.db
    year = args.year
    month = args.month
    buffer_max = args.buffer
    db_pathname = os.path.join(cache_dir, db_name)

    # init the data
    downloader = Downloader.from_source(source_name)
    dao = Dao.load_table(db_pathname, source_name, buffer_max=buffer_max)
    # download the file
    urls = downloader.get_month_files(year, month)
    for current_url in urls:
        downloader.download_file(current_url, cache_dir)
        # untar it
        tar_pathname = os.path.join(cache_dir, os.path.basename(current_url))
        tar = tarfile.open(tar_pathname, mode='r')
        names = list(map(lambda x: os.path.join(cache_dir, x), tar.getnames()))
        tar.extractall(cache_dir)
        tar.close()
        # clean it after untaring
        os.remove(tar_pathname)
        # read avro and save it in the dao
        jobs = []
        all_counter = multiprocessing.Value('i', 0)
        worker_number = multiprocessing.Value('i', 0)
        lock = multiprocessing.Lock()
        for name in names:
            p = multiprocessing.Process(target=avro_worker, args=(lock, name))
            jobs.append(p)
            p.start()

        for p in jobs:
            p.join()
