from scrapy.exporters import CsvItemExporter
from .settings import CSV_SEP

class CsvCustomSeperator(CsvItemExporter):
    def __init__(self, *args, **kwargs):
        kwargs['encoding'] = 'utf-8'
        kwargs['delimiter'] = CSV_SEP
        super(CsvCustomSeperator, self).__init__(*args, **kwargs)