import csv
import cStringIO
import codecs
import re


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, headers=None, dialect=csv.excel, encoding="utf-8", add_bom=False, **kwds):
        # Redirect output to a queue
        self.encoding = encoding
        self.headers = headers
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)

        if isinstance(f, str) or isinstance(f, unicode):
            self.path = f
            f = codecs.open(f, 'w', encoding)

        self.stream = f

        if add_bom:
            self.add_bom()

        if headers:
            self.writerow(headers)

    def add_bom(self):

        encoding = self.encoding.upper()
        encoding = re.sub('UTF[-_](\d+)', r'UTF\1', encoding)

        bom = [getattr(codecs, name) for name in dir(codecs) if isinstance(name, str) and name.endswith(encoding)][0]
        if bom:
            self.stream.write(bom)

    def write_value(self, value):
        row = [value[column] for column in self.headers]
        self.writerow(row)

    def write_values(self, values):
        for value in values:
            self.write_value(value)

    def writerow(self, row):
        if self.headers and isinstance(row, dict):
            row = [row[column] for column in self.headers]

        self.writer.writerow([unicode(s).encode(self.encoding) for s in row])

        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode(self.encoding)
        # ... and reencode it into the target encoding
        # data = self.encoder.encode(data)
        # write to the target stream
        #print [data]
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def close(self):
        self.stream.close()

    def __exit__(self):
        self.close()


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    returned = None
    undo = False

    def __init__(self, f, has_header=False, dialect=csv.excel, encoding="utf-8", **kwds):
        if isinstance(f, str) or isinstance(f, unicode):
            f = open(f, 'r')

        self.steam = f

        self.bom_process()

        self.reader = csv.reader(f, dialect=dialect, **kwds)
        self.encoding = encoding
        self.has_header = has_header

        if self.has_header:
            row = self.reader.next()
            self.headers = [unicode(s, self.encoding) for s in row]

    def bom_process(self):
        boms = [getattr(codecs, name) for name in dir(codecs) if name.startswith('BOM')]
        self.steam.seek(0)
        for bom in boms:
            i = len(bom)
            if bom == self.steam.read(i):
                return
            self.steam.seek(0)
        return

    def rewind(self):
        self.undo = True

    def next(self):
        if self.undo == True:
            self.undo = False
            return self.returned

        row = self.reader.next()
        value = [unicode(s, self.encoding) for s in row]

        if self.has_header:
            self.returned = dict(zip(self.headers, value))
        else:
            self.returned = value
        return self.returned

    def __iter__(self):
        return self

    def close(self):
        self.steam.close()

    def __enter__(self):
        return self.reader

    def __exit__(self, type, value, trackback):
        self.close()
