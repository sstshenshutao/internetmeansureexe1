import multiprocessing
import os
import sys
import argparse
import string
import time

from pandao import Dao
from http_query import Downloader
from fastavro import reader as avro_reader
import tarfile

# predefined data: can be modified by the command line args
default_source_name = string.capwords('alexa')
default_cache_dir = 'data'
default_db_name = 'example.db'
default_year = 2020
default_month = 10
default_buffer_max = 0
all_counter = multiprocessing.Value('i', 0)
worker_number = multiprocessing.Value('i', 0)


def filter_record(record):
    return str(record['query_name']).startswith('www') and (
            record['response_type'] == 'AAAA' or record['response_type'] == 'A' or
            record['response_type'] == 'CNAME')


# the worker for the avro file, each worker works for a single avro file that is extracted from the tar
def avro_worker(lck, tar_file_name, dao):
    with worker_number.get_lock():
        worker_number.value += 1
    with open(tar_file_name, 'rb') as fo:
        avro_io = avro_reader(fo)
        counter = 0
        # do not use buffer size: means all the data will be loaded, may lead to memory overflow
        if dao.buffer_max == 0:
            print(
                "\r extracting %s... [worker number: %d]" % (
                    tar_file_name, worker_number.value))
            all_records = [record for record in avro_io if filter_record(record)]
            counter += len(all_records)
            dao.insert_all_records(all_records, lck)
        else:
            for record in avro_io:
                if filter_record(record):
                    if counter % dao.buffer_max == 0:
                        if counter != 0:
                            with all_counter.get_lock():
                                all_counter.value += dao.buffer_max
                        sys.stdout.write(
                            "\r extracting %s: %d... [worker number: %d]" % (
                                tar_file_name, all_counter.value, worker_number.value))
                        sys.stdout.flush()
                    dao.insert_data(record, lck)
                    counter += 1
            dao.flush(lck)
            with all_counter.get_lock():
                all_counter.value += (counter % dao.buffer_max)
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
    parser.add_argument('--day', metavar='day', type=int, nargs='?', default=-1,
                        help='the day to query, if given, only one day will be handled')
    parser.add_argument('--db', metavar='db_name', type=str, nargs='?', default=default_db_name,
                        help='the db name (name only), example: "example.db"')
    parser.add_argument('--cache', metavar='cache_dir', type=str, nargs='?', default=default_cache_dir,
                        help='the cache folder, example: data')
    parser.add_argument('--buffer', metavar='buffer_max', type=int, nargs='?', default=default_buffer_max,
                        help='the max buffer size: the size of avro entries that are buffered in the memory')
    return parser.parse_args()


def handle_one_month(year, month, db_name=default_db_name, source_name=default_source_name,
                     cache_dir=default_cache_dir, buffer_max=default_buffer_max):
    # init the downloader
    downloader = Downloader.from_source(source_name)
    # download the files
    urls = downloader.get_month_files(year, month)
    for url in urls:
        downloader.download_file(url, cache_dir)
        # extract from the avro
        extract_one_file(os.path.basename(url), db_name, source_name, cache_dir, buffer_max)


def handle_one_day(year, month, day, db_name=default_db_name, source_name=default_source_name,
                   cache_dir=default_cache_dir, buffer_max=default_buffer_max):
    # init the downloader
    downloader = Downloader.from_source(source_name)
    # download the file
    url = downloader.get_day_file(year, month, day)
    downloader.download_file(url, cache_dir)
    # extract from the avro
    extract_one_file(os.path.basename(url), db_name, source_name, cache_dir, buffer_max)


def extract_one_file(filename, db_name=default_db_name, source_name=default_source_name, cache_dir=default_cache_dir,
                     buffer_max=default_buffer_max):
    # untar it
    tar_pathname = os.path.join(cache_dir, filename)
    tar = tarfile.open(tar_pathname, mode='r')
    names = list(map(lambda x: os.path.join(cache_dir, x), tar.getnames()))
    tar.extractall(cache_dir)
    tar.close()
    # clean it after untaring
    os.remove(tar_pathname)
    # read avro and save it in the dao
    jobs = []
    lock = multiprocessing.Lock()
    # init dao and pass it to the multiprocess
    db_pathname = os.path.join(cache_dir, db_name)
    dao = Dao.load_table(db_pathname, source_name, buffer_max=buffer_max)
    # test the time
    start_time = time.time()
    for name in names:
        p = multiprocessing.Process(target=avro_worker, args=(lock, name, dao))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

    # after all processes:
    # print the execution time
    print("used time: %f, total: %d" % (time.time() - start_time, all_counter.value))
    # clean the all_counter
    with all_counter.get_lock():
        all_counter.value = 0


if __name__ == '__main__':
    args = init_argparse()
    arg_source_name = string.capwords(args.source)
    arg_cache_dir = args.cache
    arg_db_name = args.db
    arg_year = args.year
    arg_month = args.month
    arg_buffer_max = args.buffer
    arg_day = args.day
    if arg_day == -1:
        # run the whole month
        handle_one_month(arg_year, arg_month, db_name=arg_db_name, source_name=arg_source_name,
                         buffer_max=arg_buffer_max, cache_dir=arg_cache_dir)
    else:
        handle_one_day(arg_year, arg_month, 15, db_name=arg_db_name, source_name=arg_source_name,
                       buffer_max=arg_buffer_max, cache_dir=arg_cache_dir)
