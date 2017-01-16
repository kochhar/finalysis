# http://real-chart.finance.yahoo.com/table.csv?s=KOTAKBANK.BO&a=00&b=1&c=1997&d=11&e=31&f=2016&g=d&ignore=.csv
import urllib
import bhav_copy as bc

class YahooSource(object):
    """A source for csv data by symbol from yahoo."""
    file_type_prefix = 'table'
    file_type_suffix = '.csv'

    def __init__(self, symbol):
        self.url_root = 'http://real-chart.finance.yahoo.com'
        self.symbol = symbol

    def urls_for_range(self, start_date, end_date):
        """Yields a list of urls to download a symbol's historical stock data
        given a date range."""
        params = {
            's': self.symbol,
            'a': start_date.month - 1,
            'b': start_date.day,
            'c': start_date.year,
            'd': end_date.month - 1,
            'e': end_date.day,
            'f': end_date.year,
            'g': 'd',
            'ignore': '.csv'
        }
        query = urllib.urlencode(params)
        yield '%(url_root)s/%(prefix)s%(suffix)s?%(query)s' % ({'url_root': self.url_root,
                                                                'prefix': self.file_type_prefix,
                                                                'suffix': self.file_type_suffix,
                                                                'query': query})
