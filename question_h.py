import datetime

from http_query import Downloader

alexa_top_pattern_before = "https://toplists.net.in.tum.de/archive/alexa/alexa-top1m-%Y-%m-%d.csv.xz"
alexa_top_pattern_after = "https://toplists.net.in.tum.de/archive/alexa/alexa-top1m-%Y-%m-%d_0900_UTC.csv.xz"
alexa_top_pattern_0523 = "https://toplists.net.in.tum.de/archive/alexa/alexa-top1m-2018-05-23_1000_UTC.csv.xz"
umbrella_top_pattern_before = "https://toplists.net.in.tum.de/archive/umbrella/cisco-umbrella-top1m-%Y-%m-%d.csv.xz"
umbrella_top_pattern_after = "https://toplists.net.in.tum.de/archive/umbrella/cisco-umbrella-top1m-%Y-%m-%d_0900_UTC.csv.xz"
umbrella_top_pattern_0523 = "https://toplists.net.in.tum.de/archive/umbrella/cisco-umbrella-top1m-2018-05-23_1000_UTC.csv.xz"

day = 5
month = 5
year = 2020
source = 'alexa'
cache_dir = 'data'
t = datetime.date(year, month, day)

# download
if t == datetime.date(2018, 5, 23):
    if source == 'alexa':
        Downloader.download_file(t.strftime(alexa_top_pattern_0523), cache_dir)
    elif source == 'umbrella':
        Downloader.download_file(t.strftime(umbrella_top_pattern_0523), cache_dir)
elif t > datetime.date(2018, 5, 23):
    if source == 'alexa':
        Downloader.download_file(t.strftime(alexa_top_pattern_after), cache_dir)
    elif source == 'umbrella':
        Downloader.download_file(t.strftime(umbrella_top_pattern_after), cache_dir)
else:
    if source == 'alexa':
        Downloader.download_file(t.strftime(alexa_top_pattern_before), cache_dir)
    elif source == 'umbrella':
        Downloader.download_file(t.strftime(umbrella_top_pattern_before), cache_dir)
