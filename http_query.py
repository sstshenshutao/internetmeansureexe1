import calendar
import datetime
import os
import sys

import requests

umbrella_url_pattern = "https://data.openintel.nl/data/umbrella1m/%Y/openintel-umbrella1m-%Y%m%d.tar"
alexa_url_pattern = "https://data.openintel.nl/data/alexa1m/%Y/openintel-alexa1m-%Y%m%d.tar"


class Downloader:
    @classmethod
    def from_source(cls, source_name):
        if source_name == 'Alexa':
            return cls(source_name, alexa_url_pattern)
        elif source_name == 'Umbrella':
            return cls(source_name, umbrella_url_pattern)
        else:
            raise Exception("only support Alexa and Umbrella")

    def __init__(self, source_name, url_pattern):
        self._source_name = source_name
        self._url_pattern = url_pattern

    def get_month_files(self, year, month):
        num = calendar.monthrange(year, month)[1]
        days = [datetime.date(year, month, day) for day in range(1, num + 1)]
        filenames = list(map(lambda x: x.strftime(self._url_pattern), days))
        # print(filenames)
        return filenames

    # download and re
    def download_file(self, url, cache_dir="data"):
        file_name = os.path.basename(url)
        with open(os.path.join(cache_dir, file_name), "wb") as f:
            response = requests.get(url, stream=True)
            content_length = response.headers.get('content-length')
            if content_length is None:  # no content length header
                print("no content length header")
                f.write(response.content)
            else:
                finished = 0
                content_length = int(content_length)
                for data in response.iter_content(chunk_size=4096):
                    finished += len(data)
                    f.write(data)
                    done = int(50 * finished / content_length)
                    sys.stdout.write(
                        "\r Downloading %s: %d%% [%s%s] %d/%d" % (file_name,
                                                                  float(finished) / float(content_length) * 100,
                                                                  '=' * done, ' ' * (50 - done), finished,
                                                                  content_length))
                    sys.stdout.flush()
        print("\r Downloading %s ... ok" % file_name)


if __name__ == '__main__':
    downloader = Downloader.from_source('Umbrella')
    urls = downloader.get_month_files(2020, 2)
    downloader.download_file(urls[0])
