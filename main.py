import multiprocessing
import os
import sys

from dao import Dao
from http_query import Downloader
from fastavro import reader as avro_reader
import tarfile


def inside_tar_worker(lck, tar_file_name):
    with worker_number.get_lock():
        worker_number.value += 1
    with open(tar_file_name, 'rb') as fo:
        avro_io = avro_reader(fo)
        counter = 0
        for record in avro_io:
            if record['response_type'] == 'AAAA' or record['response_type'] == 'A' or record[
                'response_type'] == 'CNAME':
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
            "extracting %s: %d...ok " % (tar_file_name, counter))
    with worker_number.get_lock():
        worker_number.value -= 1


if __name__ == '__main__':
    source_name = 'Alexa'
    cache_dir = "data"  # default:data
    db_name = 'example.db'
    year = 2020
    month = 2
    buffer_max = 10000
    db_pathname = os.path.join(cache_dir, db_name)

    # init the data
    downloader = Downloader.from_source(source_name)
    dao = Dao.load_table(db_pathname, source_name, buffer_max=buffer_max)
    # download the file
    urls = downloader.get_month_files(year, month)[:1]
    for current_url in urls:
        # downloader.download_file(current_url, cache_dir)
        # untar it
        tar_pathname = os.path.join(cache_dir, os.path.basename(current_url))
        tar = tarfile.open(tar_pathname, mode='r')
        names = list(map(lambda x: os.path.join(cache_dir, x), tar.getnames()))
        tar.extractall(cache_dir)
        tar.close()
        # read avro and save it in the dao
        jobs = []
        all_counter = multiprocessing.Value('i', 0)
        worker_number = multiprocessing.Value('i', 0)
        lock = multiprocessing.Lock()
        for name in names:
            p = multiprocessing.Process(target=inside_tar_worker, args=(lock, name))
            jobs.append(p)
            p.start()

        for p in jobs:
            p.join()
