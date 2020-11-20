import multiprocessing
import os
import sys

from dao import Dao
from http_query import Downloader
from fastavro import reader as avro_reader
import tarfile

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
        for name in tar.getnames():
            avro_io = avro_reader(tar.extractfile(name))
            counter = 0
            for record in avro_io:
                if counter % buffer_max == 0:
                    sys.stdout.write(
                        "\r extracting %s: %d..." % (name, counter))
                    sys.stdout.flush()
                dao.insert_data(record)
                counter += 1
            dao.flush()
            sys.stdout.write(
                "\r extracting %s: %d...ok" % (name, counter))
        tar.close()
        # read avro and save it in the dao
