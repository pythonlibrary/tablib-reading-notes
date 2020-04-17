""" Tablib - *SV Support.
"""

import csv
from io import StringIO


class CSVFormat:
    title = 'csv'
    extensions = ('csv',)

    DEFAULT_DELIMITER = ','

    # pythonlibrary.net: 
	# 这些方法必须设置为classmethod, 因为我们动态的将format类注册上去
	# 而即使想JSONFormat这种通过实例注册的，也可以使用classmethod
    @classmethod
    def export_stream_set(cls, dataset, **kwargs):
        """Returns CSV representation of Dataset as file-like."""
        stream = StringIO()

        # pythonlibrary.net: 
		# 使用setdefault，增加一组key value到字典中，如果在字典中key已经存在
		# 则什么都不做，如果key不存在，则创建新的
        kwargs.setdefault('delimiter', cls.DEFAULT_DELIMITER)

        _csv = csv.writer(stream, **kwargs)

        for row in dataset._package(dicts=False):
            _csv.writerow(row)

        stream.seek(0)
        return stream

    @classmethod
    def export_set(cls, dataset, **kwargs):
        """Returns CSV representation of Dataset."""
        stream = cls.export_stream_set(dataset, **kwargs)
        return stream.getvalue()

    @classmethod
    def import_set(cls, dset, in_stream, headers=True, **kwargs):
        """Returns dataset from CSV stream."""

        dset.wipe()

        kwargs.setdefault('delimiter', cls.DEFAULT_DELIMITER)

        rows = csv.reader(in_stream, **kwargs)
        for i, row in enumerate(rows):

            if (i == 0) and (headers):
                dset.headers = row
            elif row:
                if i > 0 and len(row) < dset.width:
                    row += [''] * (dset.width - len(row))
                dset.append(row)

    @classmethod
    def detect(cls, stream, delimiter=None):
        """Returns True if given stream is valid CSV."""
        try:
            csv.Sniffer().sniff(stream.read(1024), delimiters=delimiter or cls.DEFAULT_DELIMITER)
            return True
        except Exception:
            return False
