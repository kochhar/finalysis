from __future__ import print_function
import collections
import datetime
import functools
import itertools as it
import logging
import math
from os import path
import requests
import sys
import time
import urlparse


log = logging.getLogger()
DL = collections.namedtuple('Download', ['url', 'rfile', 'lfile', 'status'])


class Source(object):
    """A source for bhav copy."""

    def urls_for_range(self, start_date, end_date):
        """Yields a list of urls to download all bhav copy data in the interval
        from the first date to the second date - [from, to).

        Params
        ------
          start_date: datetime from when to start downloading
          end_date: datetime until when to download
        """
        current_date = start_date
        one_day = datetime.timedelta(days=1)

        while current_date < end_date:
            # Don't yield saturday or sunday
            if current_date.weekday() not in [5, 6]:
                yield self.url_for_date(current_date)

            current_date = current_date + one_day


class EquitySource(Source):
    """Downloader class which helps to download a bhav copy file."""
    file_type_prefix = 'eq'
    file_type_suffix = '_csv.zip'

    def __init__(self):
        self.url_root = "http://www.bseindia.com/download/BhavCopy/Equity/"

    def url_for_date(self, date):
        """Returns the downlaod url for a given date."""
        dated_parts = [self.file_type_prefix, date.strftime("%d%m%y"), self.file_type_suffix]
        dated_file = ''.join(dated_parts)
        return (urlparse.urljoin(self.url_root, dated_file), dated_file)


class ExponentialBackoff(object):
    """Simple backoff state management with exponential increase/decrease in interval."""
    def __init__(self, start, limit):
        self.count = 0
        self.start = start
        self.limit = limit

    @property
    def interval(self):
        return min(self.limit, self.start*math.pow(2, self.count))

    def backoff(self):
        log.info('Backing off for %0.2f(s)', self.interval)
        time.sleep(self.interval)
        self.count += 1

    def wait(self):
        interval = max(0.01, self.start*self.count)
        if interval > 0.01:
            log.info('Waiting without back off for %0.2f(s)', interval)
            time.sleep(interval)
            self.count *= 0.8705     # this will bring count down by 50% every 5 calls


def partition(pred, iterable):
    """Split an iterable using a predicate. Returns [pred(x) == False], [pred(x) == True]"""
    t1, t2 = it.tee(iterable)
    return list(it.ifilterfalse(pred, t1)), filter(pred, t2)


def download(sources, dest_dir, attempt=1):
    """Download all the urls specified in the (filename, url) pairs contained
    in source iter. Each downloaded item will be saved in filename."""
    retry_on_fail = attempt <= 3
    downloader = functools.partial(requests.get, stream=True)

    results = _download(sources, dest_dir, downloader)
    fails, successes = partition(lambda r: r.lfile is not None, results)
    retryables = [f for f in fails if not f.status == 404]

    if retryables and retry_on_fail:
        # retry any items which were not missing (i.e. not 404)
        retry_fail, retry_success = download([(r.url, r.rfile) for r in retryables], dest_dir,
                                             attempt+1)
        successes += retry_success
        success_urls = set(s.url for s in retry_success)
        fails = [f for f in fails if f.url not in success_urls]
    elif retryables:  # and not retry_fail
        log.info('Giving up with %d failures', len(fails))

    print('Downloaded %d files, %d success, %d failed' %
          (len(results), len(successes), len(fails)))
    return fails, successes


def _download(sources, dest_dir, downloader, retries=3):
    """Download all the items in the source using the specified downloader.

    Params
    ------
      downloader: a downloader function which accepts a url to download as
      input and returns a Response object with an iter_content(block_size)
      method.
    """
    exp_back = ExponentialBackoff(0.5, 64)

    results = []
    for i, (item, dated_file) in enumerate(sources):
        print('Processing file %s:%s' % (i, dated_file), end='\r')
        sys.stdout.flush()

        resp = downloader(item)
        if not resp.ok:
            log.warn('Error in %s, status: %s', item, resp.status_code)
            results.append(DL(url=item, rfile=dated_file, lfile=None, status=resp.status_code))
            if resp.status_code == 401:
                exp_back.backoff()
            continue

        filename = path.abspath(path.join(dest_dir, dated_file))
        with open(filename, 'wb') as handle:
            for block in resp.iter_content(256*1024):
                handle.write(block)
        results.append(DL(url=item, rfile=dated_file, lfile=filename, status=resp.status_code))
        exp_back.wait()

    return results


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Download daily equity bhav copy between dates')
    parser.add_argument('start', type=str, help='A start date for downloading')
    parser.add_argument('end', type=str, help='An end date for downloading')
    parser.add_argument('-o', '--out', dest='out', type=str, default='.',
                        help='The destination directory for the output, default is the current dir')

    args = parser.parse_args()

    date_parser = datetime.datetime.strptime
    date_format = '%Y-%m-%d'
    start, end = date_parser(args.start, date_format), date_parser(args.end, date_format)
    fails, successes = download(EquitySource().urls_for_range(start, end), args.out)
