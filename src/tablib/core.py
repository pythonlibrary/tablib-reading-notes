"""
    tablib.core
    ~~~~~~~~~~~

    This module implements the central Tablib objects.

    :copyright: (c) 2016 by Kenneth Reitz. 2019 Jazzband.
    :license: MIT, see LICENSE for more details.
"""

from collections import OrderedDict
from copy import copy
from operator import itemgetter

from tablib.exceptions import (
    HeadersNeeded,
    InvalidDatasetIndex,
    InvalidDatasetType,
    InvalidDimensions,
    UnsupportedFormat,
)
from tablib.formats import registry
from tablib.utils import normalize_input

__title__ = 'tablib'
__author__ = 'Kenneth Reitz'
__license__ = 'MIT'
__copyright__ = 'Copyright 2017 Kenneth Reitz. 2019 Jazzband.'
__docformat__ = 'restructuredtext'


class Row:
    """Internal Row object. Mainly used for filtering."""

    __slots__ = ['_row', 'tags']

    def __init__(self, row=list(), tags=list()):
        self._row = list(row)
        self.tags = list(tags)

    def __iter__(self):
        # pythonlibrary.net: __iter__ 方法是用来支持 "for x in row"这样的语法的
        return (col for col in self._row)

    def __len__(self):
        # pythonlibrary.net: __len__ 方法是用来支持"len(row)"这样的语法的
        return len(self._row)

    def __repr__(self):
        # pythonlibrary.net: __repr__ 方法用来支持调试器
        # 1. 如果使用调试器观察row对象的话，可以同时显示这里的信息
        # 2. 如果一个类没没有定义__str__方法，那么__repr__会被用来替代__str__方法，而__str__方法的作用是
        #    当使用print(row)来打印row对象的时候，会打印出__str__方法的返回内容
        return repr(self._row)

    def __getitem__(self, i):
        # pythonlibrary.net: __getitem__ 方法用来支持通过[]操作符来获取对象中的元素，例如"var = row[1]"
        return self._row[i]

    def __setitem__(self, i, value):
        # pythonlibrary.net: __setitem__ 方法用来支持通过[]操作符来修改对象中的元素，例如"row[1] = 1234"
        self._row[i] = value

    def __delitem__(self, i):
        # pythonlibrary.net: __delitem__ 方法用来支持通过del来删除[]操作符选中的元素, 例如 "del row[1]"
        del self._row[i]

    def __getstate__(self):
        # pythonlibrary.net:  __getstate__ 和 __setstate__ 方法用来支持 pickle 库

        slots = dict()

        for slot in self.__slots__:
            attribute = getattr(self, slot)
            slots[slot] = attribute

        return slots

    def __setstate__(self, state):
        # pythonlibrary.net:  __getstate__ 和 __setstate__ 方法用来支持 pickle 库
        for (k, v) in list(state.items()):
            setattr(self, k, v)

    def rpush(self, value):
        self.insert(len(self._row), value)

    def lpush(self, value):
        self.insert(0, value)

    def append(self, value):
        self.rpush(value)

    def insert(self, index, value):
        self._row.insert(index, value)

    def __contains__(self, item):
        # pythonlibrary.net: __contains__ 方法支持 "if item in row" 的语法
        return (item in self._row)

    @property
    def tuple(self):
        """Tuple representation of :class:`Row`."""
        return tuple(self._row)

    @property
    def list(self):
        """List representation of :class:`Row`."""
        return list(self._row)

    def has_tag(self, tag):
        """Returns true if current row contains tag."""

        if tag is None:
            return False
        elif isinstance(tag, str):
            return (tag in self.tags)
        else:
            return bool(len(set(tag) & set(self.tags)))


class Dataset:
    """The :class:`Dataset` object is the heart of Tablib. It provides all core
    functionality.

    Usually you create a :class:`Dataset` instance in your main module, and append
    rows as you collect data. ::

        data = tablib.Dataset()
        data.headers = ('name', 'age')

        for (name, age) in some_collector():
            data.append((name, age))


    Setting columns is similar. The column data length must equal the
    current height of the data and headers must be set. ::

        data = tablib.Dataset()
        data.headers = ('first_name', 'last_name')

        data.append(('John', 'Adams'))
        data.append(('George', 'Washington'))

        data.append_col((90, 67), header='age')


    You can also set rows and headers upon instantiation. This is useful if
    dealing with dozens or hundreds of :class:`Dataset` objects. ::

        headers = ('first_name', 'last_name')
        data = [('John', 'Adams'), ('George', 'Washington')]

        data = tablib.Dataset(*data, headers=headers)

    :param \\*args: (optional) list of rows to populate Dataset
    :param headers: (optional) list strings for Dataset header row
    :param title: (optional) string to use as title of the Dataset


    .. admonition:: Format Attributes Definition

     If you look at the code, the various output/import formats are not
     defined within the :class:`Dataset` object. To add support for a new format, see
     :ref:`Adding New Formats <newformats>`.

    """

    def __init__(self, *args, **kwargs):
        self._data = list(Row(arg) for arg in args)
        self.__headers = None

        # ('title', index) tuples
        self._separators = []

        # (column, callback) tuples
        self._formatters = []

        self.headers = kwargs.get('headers')

        self.title = kwargs.get('title')

    def __len__(self):
        # pythonlibrary.net: self.height 是下边定义的一个property
        return self.height

    def __getitem__(self, key):
        if isinstance(key, str):
            # pythonlibrary.net:
            # 如果key是一个字符串，则检查key是不是在self.headers中出现，如果出现了，就返回那一列的内容
            if key in self.headers:
                pos = self.headers.index(key)  # get 'key' index from each data
                return [row[pos] for row in self._data]
            else:
                raise KeyError
        else:
            # pythonlibrary.net:
            # 如果key不是一个字符串，则它有可能是一个数字或者是一个切片操作
            _results = self._data[key]
            if isinstance(_results, Row):
                # pythonlibrary.net: 
                # 如果使用key获得的是一个Row对象，那说明key是一个数字，我们获得了某一行，然后利用tuple获得
                # 该行数据的tuple表示
                return _results.tuple
            else:
                # pythonlibrary.net: 
                # 如果key是类似于[0:2]这样的切片操作，我们将获得多行对象，最终返回一个list，每一个元素是一个
                # 该行数据的tuple表示
                return [result.tuple for result in _results]

    def __setitem__(self, key, value):
        # pythonlibrary.net: 
        # 这个方法用来支持使用[]操作符设置元素，类似于dataset[1] = xxxx的操作，可以用来修改某一行的值
        # 因此我们要先检查输入值得有效性
        # 它跟__getitem__方法不同，__getitem__方法支持通过标题来获取列数据，而__setitem__方法仅支持修改行
        self._validate(value)
        # pythonlibrary.net: 如果输入的值有效，则修改指定行的内容 
        self._data[key] = Row(value)

    def __delitem__(self, key):
        # pythonlibrary.net: 
        # 跟__getitem__方法类似, 我们可以通过[]操作符来删除某一行或者某一列
        if isinstance(key, str):

            if key in self.headers:

                pos = self.headers.index(key)
                del self.headers[pos]

                for i, row in enumerate(self._data):
                    # pythonlibrary.net: 
                    # 因为Dataset中的数据是按行保存的，当我们要删除某一列的时候，需要循环删除每一行中的对应列
                    del row[pos]
                    self._data[i] = row
            else:
                raise KeyError
        else:
            del self._data[key]

    def __repr__(self):
        # pythonlibrary.net: 
        # 这个我们在Row类中解释过了
        try:
            return '<%s dataset>' % (self.title.lower())
        except AttributeError:
            return '<dataset object>'

    def __str__(self):
        # pythonlibrary.net: 
        # __str__ 方法用来支持 'print(dataset)'，所以这里代码较多，因为在用户打印一个dataset的时候
        # tablib将按表格的形式将数据输出到终端
        result = []

        # Add str representation of headers.
        if self.__headers:
            result.append([str(h) for h in self.__headers])

        # Add str representation of rows.
        # pythonlibrary.net: 
        # map函数将会把row里边的每一个元素作为参数传递给str()函数，最终每一行会变成一个字符串list被添加到result列表中
        result.extend(list(map(str, row)) for row in self._data)

        # pythonlibrary.net
        # 下边lens和fields_lens是用来调整输出格式，保证输出列的宽度足够放所有的列元素，美化输出
        lens = [list(map(len, row)) for row in result] # pythonlibrary.net: 获得每一行中每一个元素的长度
        field_lens = list(map(max, zip(*lens))) # pythonlibrary.net: 获得每一列中元素的最大宽度

        # delimiter between header and data
        if self.__headers:
            result.insert(1, ['-' * length for length in field_lens])

        # pythonlibrary.net: 
        # 使用每一个列的最大宽度，创建了一个格式化字符串，该字符串的内容类似于 {0:7}|{1:5} 
        # 这是python格式化字符串的一种语法，它的含义是第一例的宽度为7，第二列的宽度为5
        format_string = '|'.join('{%s:%s}' % item for item in enumerate(field_lens)) 

        # pythonlibrary.net: 
        # 使用上边生成的格式字符串来格式化每一行，然后放在一个list里边，最后通过 \n 把他们连接起来，从而实现了
        # 换行的目的
        return '\n'.join(format_string.format(*row) for row in result)

    # ---------
    # Internals
    # ---------

    def _get_in_format(self, fmt_key, **kwargs):
        # pythonlibrary.net: 
        # 调用了format类的export_set方法，其具体实现要看不同的format类
        return registry.get_format(fmt_key).export_set(self, **kwargs)

    def _set_in_format(self, fmt_key, in_stream, **kwargs):
        # pythonlibrary.net: 
        # 调用了format类的import_set方法，其具体实现要看不同的format类
        in_stream = normalize_input(in_stream)
        return registry.get_format(fmt_key).import_set(self, in_stream, **kwargs)

    def _validate(self, row=None, col=None, safety=False):
        """Assures size of every row in dataset is of proper proportions."""
        # pythonlibrary.net: 
        # 检查传入的数据格式是否正确
        if row:
            # pythonlibrary.net: 
            # 如果dataset的width属性是有值的，也就是说如果dataset中有header或者有数据
            # 则确认给定的行中的数据个数是不是跟dataset宽度（也就是每一行的元素个数）一致
            # 如果dataset的width属性没值，那么说明dataset中没数据，因此无论给定的数据中
            # 有多少个数据都可以作为第一行
            is_valid = (len(row) == self.width) if self.width else True
        elif col:
            # pythonlibrary.net:
            # 给定的列的数据个数跟dataset的行数一致，则认为数据有效
            # 若dataset中没有数据，则认为给定的列为第一列
            if len(col) < 1:
                is_valid = True
            else:
                is_valid = (len(col) == self.height) if self.height else True
        else:
            # pythonlibrary.net: 
            # all函数用来检查list中的所有元素是否为True
            is_valid = all(len(x) == self.width for x in self._data)

        if is_valid:
            return True
        else:
            # pythonlibrary.net: 
            # 如果safety参数设置为True，则及时格式不正确也不会抛出异常
            if not safety:
                raise InvalidDimensions
            return False

    def _package(self, dicts=True, ordered=True):
        """Packages Dataset into lists of dictionaries for transmission."""
        # TODO: Dicts default to false?

        _data = list(self._data)

        if ordered:
            dict_pack = OrderedDict
        else:
            dict_pack = dict

        # Execute formatters
        if self._formatters:
            for row_i, row in enumerate(_data):
                for col, callback in self._formatters:
                    try:
                        if col is None:
                            for j, c in enumerate(row):
                                # pythonlibrary.net: 
                                # 如果没有提供列名，则针对该每一行的所有元素进行格式化
                                # callback就是格式化回调函数
                                _data[row_i][j] = callback(c)
                        else:
                                # pythonlibrary.net: 
                                # 如果提供了列名，那么只用callback来格式化给定列的元素
                            _data[row_i][col] = callback(row[col])
                    except IndexError:
                        raise InvalidDatasetIndex

        if self.headers:
            if dicts:
                # pythonlibrary.net: When dicts is enabled, that means we want to package data to dicts
                #      create dicts using the header as key and row as value for each row, one row for one dict
                #      data will be a list that holds all the dict that represents the row
                data = [dict_pack(list(zip(self.headers, data_row))) for data_row in _data]
            else:
                # pythonlibrary.net: When dicts is disabled, that means we want to package data to list
                data = [list(self.headers)] + list(_data)
        else:
            # There are no headers, we could only package the data to list
            data = [list(row) for row in _data]

        return data

    def _get_headers(self):
        """An *optional* list of strings to be used for header rows and attribute names.

        This must be set manually. The given list length must equal :class:`Dataset.width`.

        """
        return self.__headers

    def _set_headers(self, collection):
        """Validating headers setter."""
        # pythonlibrary.net: 
        # 因为在调用_validate方法的时候选择了non-saftey方式，因此如果格式不正确会抛出异常，所以
        # 这里不需要判断该方法的返回值
        self._validate(collection)
        if collection:
            try:
                self.__headers = list(collection)
            except TypeError:
                raise TypeError
        else:
            self.__headers = None

    # pythonlibrary.net: 
    # 使用了property装饰器将_get_headers和_set_headers 设置成了getter和setter，在访问dataset
    # 的headers属性会调用这两个方法
    headers = property(_get_headers, _set_headers)

    def _get_dict(self):
        """A native Python representation of the :class:`Dataset` object. If headers have
        been set, a list of Python dictionaries will be returned. If no headers have been set,
        a list of tuples (rows) will be returned instead.

        A dataset object can also be imported by setting the `Dataset.dict` attribute: ::

            data = tablib.Dataset()
            data.dict = [{'age': 90, 'first_name': 'Kenneth', 'last_name': 'Reitz'}]

        """
        # pythonlibrary.net: 
        # 调用_package方法将daset里边的数据转换给dict类型
        return self._package()

    def _set_dict(self, pickle):
        """A native Python representation of the Dataset object. If headers have been
        set, a list of Python dictionaries will be returned. If no headers have been
        set, a list of tuples (rows) will be returned instead.

        A dataset object can also be imported by setting the :class:`Dataset.dict` attribute. ::

            data = tablib.Dataset()
            data.dict = [{'age': 90, 'first_name': 'Kenneth', 'last_name': 'Reitz'}]

        """

        if not len(pickle):
            return

        # if list of rows
        if isinstance(pickle[0], list):
            self.wipe()
            for row in pickle:
                self.append(Row(row))

        # if list of objects
        elif isinstance(pickle[0], dict):
            self.wipe()
            self.headers = list(pickle[0].keys())
            for row in pickle:
                self.append(Row(list(row.values())))
        else:
            raise UnsupportedFormat

    dict = property(_get_dict, _set_dict)

    def _clean_col(self, col):
        """Prepares the given column for insert/append."""

        col = list(col)

        # pythonlibrary.net: 
        # 获得定列的表头
        if self.headers:
            header = [col.pop(0)]
        else:
            header = []

        # pythonlibrary.net: 
        # 如果给定列是一个函数，则使用该函数处理每一行
        # 注：这里主要是用来支持文章中提到的dynamic column功能
        #     即，可以使用一个函数来生成列，这个函数有一个固定参数，参数为Row类型
        if len(col) == 1 and hasattr(col[0], '__call__'):

            col = list(map(col[0], self._data))
        col = tuple(header + col)

        return col

    @property
    def height(self):
        """The number of rows currently in the :class:`Dataset`.
           Cannot be directly modified.
        """
        return len(self._data)

    @property
    def width(self):
        """The number of columns currently in the :class:`Dataset`.
           Cannot be directly modified.
        """
        # pythonlibrary.net: 
        # dataset的宽度，先尝试使用第一行的元素个数
        # 如果失败了，则使用表头的元素个数，如果表头也不存在使用0
        try:
            return len(self._data[0])
        except IndexError:
            try:
                return len(self.headers)
            except TypeError:
                return 0

    def load(self, in_stream, format=None, **kwargs):
        """
        Import `in_stream` to the :class:`Dataset` object using the `format`.
        `in_stream` can be a file-like object, a string, or a bytestring.

        :param \\*\\*kwargs: (optional) custom configuration to the format `import_set`.
        """

        stream = normalize_input(in_stream)
        if not format:
            # pythonlibrary.net: 
            # 如果没有提供格式，则尝试自动检测
            format = detect_format(stream)

        fmt = registry.get_format(format)
        if not hasattr(fmt, 'import_set'):
            raise UnsupportedFormat('Format {} cannot be imported.'.format(format))

        if not import_set:
            # support to pass in the custom import_set function
            raise UnsupportedFormat('Format {} cannot be imported.'.format(format))

        fmt.import_set(self, stream, **kwargs)
        return self

    def export(self, format, **kwargs):
        """
        Export :class:`Dataset` object to `format`.

        :param \\*\\*kwargs: (optional) custom configuration to the format `export_set`.
        """

        fmt = registry.get_format(format)
        if not hasattr(fmt, 'export_set'):
            raise UnsupportedFormat('Format {} cannot be exported.'.format(format))

        return fmt.export_set(self, **kwargs)

    # ----
    # Rows
    # ----

    def insert(self, index, row, tags=list()):
        """Inserts a row to the :class:`Dataset` at the given index.

        Rows inserted must be the correct size (height or width).

        The default behaviour is to insert the given row to the :class:`Dataset`
        object at the given index.
       """

        self._validate(row)
        self._data.insert(index, Row(row, tags=tags))

    def rpush(self, row, tags=list()):
        """Adds a row to the end of the :class:`Dataset`.
        See :class:`Dataset.insert` for additional documentation.
        """

        self.insert(self.height, row=row, tags=tags)

    def lpush(self, row, tags=list()):
        """Adds a row to the top of the :class:`Dataset`.
        See :class:`Dataset.insert` for additional documentation.
        """

        self.insert(0, row=row, tags=tags)

    def append(self, row, tags=list()):
        """Adds a row to the :class:`Dataset`.
        See :class:`Dataset.insert` for additional documentation.
        """

        self.rpush(row, tags)

    def extend(self, rows, tags=list()):
        """Adds a list of rows to the :class:`Dataset` using
        :class:`Dataset.append`
        """

        for row in rows:
            self.append(row, tags)

    def lpop(self):
        """Removes and returns the first row of the :class:`Dataset`."""

        cache = self[0]
        del self[0]

        return cache

    def rpop(self):
        """Removes and returns the last row of the :class:`Dataset`."""

        cache = self[-1]
        del self[-1]

        return cache

    def pop(self):
        """Removes and returns the last row of the :class:`Dataset`."""

        return self.rpop()

    # -------
    # Columns
    # -------

    def insert_col(self, index, col=None, header=None):
        """Inserts a column to the :class:`Dataset` at the given index.

        Columns inserted must be the correct height.

        You can also insert a column of a single callable object, which will
        add a new column with the return values of the callable each as an
        item in the column. ::

            data.append_col(col=random.randint)

        If inserting a column, and :class:`Dataset.headers` is set, the
        header attribute must be set, and will be considered the header for
        that row.

        See :ref:`dyncols` for an in-depth example.

        .. versionchanged:: 0.9.0
           If inserting a column, and :class:`Dataset.headers` is set, the
           header attribute must be set, and will be considered the header for
           that row.

        .. versionadded:: 0.9.0
           If inserting a row, you can add :ref:`tags <tags>` to the row you are inserting.
           This gives you the ability to :class:`filter <Dataset.filter>` your
           :class:`Dataset` later.

        """

        if col is None:
            col = []

        # Callable Columns...
        if hasattr(col, '__call__'):
            # pythonlibrary.net: 
            # 动态列，动态列使用的函数仅支持Row作为参数
            col = list(map(col, self._data))

        # pythonlibrary.net: 
        # 传统列和动态列都经过_clean_col处理成传统数字格式的列
        col = self._clean_col(col)
        self._validate(col=col)

        if self.headers:
            # pop the first item off, add to headers
            if not header:
                # pythonlibrary.net: 
                # 如果dataset有表头，但是本函数没有提供表头则抛出异常
                raise HeadersNeeded()

            # corner case - if header is set without data
            elif header and self.height == 0 and len(col):
                # pythonlibrary.net: 
                # 如果dataset有表头，但是没有数据，同时col提供了数据
                # 则提供的数据太多抛出异常
                raise InvalidDimensions

            self.headers.insert(index, header)

        if self.height and self.width:
            # pythonlibrary.net: 
            # dataset里边有数据

            for i, row in enumerate(self._data):
                # pythonlibrary.net: 
                # 向每一行中增加列元素

                row.insert(index, col[i])
                self._data[i] = row
        else:
            # pythonlibrary.net: 
            # 如果dataset里边没有数据，则用给定的元素创建每一行的Row对象
            self._data = [Row([row]) for row in col]

    def rpush_col(self, col, header=None):
        """Adds a column to the end of the :class:`Dataset`.
        See :class:`Dataset.insert` for additional documentation.
        """

        self.insert_col(self.width, col, header=header)

    def lpush_col(self, col, header=None):
        """Adds a column to the top of the :class:`Dataset`.
        See :class:`Dataset.insert` for additional documentation.
        """

        self.insert_col(0, col, header=header)

    def insert_separator(self, index, text='-'):
        """Adds a separator to :class:`Dataset` at given index."""

        sep = (index, text)
        self._separators.append(sep)

    def append_separator(self, text='-'):
        """Adds a :ref:`separator <separators>` to the :class:`Dataset`."""

        # change offsets if headers are or aren't defined
        # pythonlibrary.net: add sperator and specify the location of the sperator
        #      if there is a header in the dataset, need to offset 1 
        if not self.headers:
            index = self.height if self.height else 0
        else:
            index = (self.height + 1) if self.height else 1

        self.insert_separator(index, text)

    def append_col(self, col, header=None):
        """Adds a column to the :class:`Dataset`.
        See :class:`Dataset.insert_col` for additional documentation.
        """

        self.rpush_col(col, header)

    def get_col(self, index):
        """Returns the column from the :class:`Dataset` at the given index."""

        return [row[index] for row in self._data]

    # ----
    # Misc
    # ----

    def add_formatter(self, col, handler):
        """Adds a formatter to the :class:`Dataset`.

        .. versionadded:: 0.9.5

        :param col: column to. Accepts index int or header str.
        :param handler: reference to callback function to execute against
                        each cell value.
        """

        if isinstance(col, str):
            if col in self.headers:
                col = self.headers.index(col)  # get 'key' index from each data
            else:
                raise KeyError

        if not col > self.width:
            # pythonlibrary.net: 
            # 每一列都可以有不同的格式
            # 注：这里的格式不同于在formats文件夹里的格式，这里指的是我们怎么来改变每一列的格式
            #     比如我们想把某一列的字符串全部大写，也就是改变那一列的格式，而formats文件夹中的
            #     格式指的是tablib支持什么样格式的文件
            self._formatters.append((col, handler))
        else:
            raise InvalidDatasetIndex

        return True

    def filter(self, tag):
        """Returns a new instance of the :class:`Dataset`, excluding any rows
        that do not contain the given :ref:`tags <tags>`.
        """
        # pythonlibrary.net: 
        # 使用tag来过滤dataset，因为我们要一个子dataset，所以需要先copy
        _dset = copy(self)
        _dset._data = [row for row in _dset._data if row.has_tag(tag)]

        return _dset

    def sort(self, col, reverse=False):
        """Sort a :class:`Dataset` by a specific column, given string (for
        header) or integer (for column index). The order can be reversed by
        setting ``reverse`` to ``True``.

        Returns a new :class:`Dataset` instance where columns have been
        sorted.
        """

        if isinstance(col, str):
            # the provided col is the header fo the 

            if not self.headers:
                raise HeadersNeeded
            # pythonlibrary.net: 
            # 1. itemgetter函数：假设 f = itemgetter(2), 那么这样调用f(r)的话其实相当于r[2]
            #                    假设 g = itemgetter(2, 5, 3), 那么这样调用g(r)的话其实相当于(r[2], r[5], r[3])
            # 2. sorted函数支持提供一个函数来在排序前对数据进行预处理，举例来说
            #      a = [1,45,2,1,5,3]
            #      sorted(a) -> [1, 1, 2, 3, 5, 45]
            #      sorted(a, key=lambda x:1/x) -> 45, 5, 3, 2, 1, 1]
            # 因此itemgetter(col)将将使用列名来通过self.dict获取元素值，然后对其排序

            _sorted = sorted(self.dict, key=itemgetter(col), reverse=reverse)
            _dset = Dataset(headers=self.headers, title=self.title)

            for item in _sorted:
                # as _sorted is a sorted dict from self.dict, so here we convert the dict back to dataset
                row = [item[key] for key in self.headers]
                _dset.append(row=row)

        else:
            # the provided col is the column number
            if self.headers:
                col = self.headers[col]

            _sorted = sorted(self.dict, key=itemgetter(col), reverse=reverse)
            _dset = Dataset(headers=self.headers, title=self.title)

            for item in _sorted:
                if self.headers:
                    row = [item[key] for key in self.headers]
                else:
                    row = item
                _dset.append(row=row)

        return _dset

    def transpose(self):
        """Transpose a :class:`Dataset`, turning rows into columns and vice
        versa, returning a new ``Dataset`` instance. The first row of the
        original instance becomes the new header row."""

        # Don't transpose if there is no data
        if not self:
            return

        _dset = Dataset()
        # The first element of the headers stays in the headers,
        # it is our "hinge" on which we rotate the data

        # pythonlibrary.net: 
        # 使用第一列（包含表头）作为新的表头
        new_headers = [self.headers[0]] + self[self.headers[0]]

        _dset.headers = new_headers
        for index, column in enumerate(self.headers):

            if column == self.headers[0]:
                # It's in the headers, so skip it
                continue

            # Adding the column name as now they're a regular column
            # Use `get_col(index)` in case there are repeated values
            row_data = [column] + self.get_col(index)
            row_data = Row(row_data)
            _dset.append(row=row_data)
        return _dset

    def stack(self, other):
        """Stack two :class:`Dataset` instances together by
        joining at the row level, and return new combined
        ``Dataset`` instance."""

        if not isinstance(other, Dataset):
            return

        if self.width != other.width:
            raise InvalidDimensions

        # Copy the source data
        _dset = copy(self)

        rows_to_stack = [row for row in _dset._data]
        other_rows = [row for row in other._data]

        rows_to_stack.extend(other_rows)
        _dset._data = rows_to_stack

        return _dset

    def stack_cols(self, other):
        """Stack two :class:`Dataset` instances together by
        joining at the column level, and return a new
        combined ``Dataset`` instance. If either ``Dataset``
        has headers set, than the other must as well."""

        if not isinstance(other, Dataset):
            return

        # pythonlibrary.net:
        # 两个dataset都需要有表头
        if self.headers or other.headers:
            if not self.headers or not other.headers:
                raise HeadersNeeded

        # pythonlibrary.net: 
        # 两个dataset的高度一致
        if self.height != other.height:
            raise InvalidDimensions

        try:
            new_headers = self.headers + other.headers
        except TypeError:
            new_headers = None

        _dset = Dataset()

        for column in self.headers:
            _dset.append_col(col=self[column])

        for column in other.headers:
            _dset.append_col(col=other[column])

        _dset.headers = new_headers

        return _dset

    def remove_duplicates(self):
        """Removes all duplicate rows from the :class:`Dataset` object
        while maintaining the original order."""
        seen = set()
        # pythonlibrary.net: 
        # 对self._data进行迭代，取出每一行，如果tuple(row)在seen里边没出现，则把tuple(row)加到seen里边
        # 并将row加到返回的list中，而如果tuple(row)在seen里边出现过则什么都不做
        # 一个简单的例子：
        #           a = [1,2,3,4]
        #           b = list()
        #           [v for v in a if v == 1 or b.append(v)] 将返回 [1],  b则为[2,3,4]
        self._data[:] = [row for row in self._data if not (tuple(row) in seen or seen.add(tuple(row)))]

    def wipe(self):
        """Removes all content and headers from the :class:`Dataset` object."""
        self._data = list()
        self.__headers = None

    def subset(self, rows=None, cols=None):
        """Returns a new instance of the :class:`Dataset`,
        including only specified rows and columns.
        """

        # Don't return if no data
        if not self:
            return

        if rows is None:
            # pythonlibrary.net: 
            # 如果没提供rows则选取所有的row
            rows = list(range(self.height))

        if cols is None:
            # pythonlibrary.net:
            # 如果没提供cols则选取所有的列
            cols = list(self.headers)

        # filter out impossible rows and columns
        # check if the givn rows and cols actually exist
        rows = [row for row in rows if row in range(self.height)]
        cols = [header for header in cols if header in self.headers]

        _dset = Dataset()

        # filtering rows and columns
        _dset.headers = list(cols)

        _dset._data = []
        for row_no, row in enumerate(self._data):
            data_row = []
            for key in _dset.headers:
                if key in self.headers:
                    pos = self.headers.index(key)
                    data_row.append(row[pos])
                else:
                    raise KeyError

            if row_no in rows:
                _dset.append(row=Row(data_row))

        return _dset


class Databook:
    """A book of :class:`Dataset` objects.
    """

    def __init__(self, sets=None):
        # pythonlibrary.net: 
        # 一个databook由多个dataset组成
        self._datasets = sets or []

    def __repr__(self):
        try:
            return '<%s databook>' % (self.title.lower())
        except AttributeError:
            return '<databook object>'

    def wipe(self):
        """Removes all :class:`Dataset` objects from the :class:`Databook`."""
        self._datasets = []

    def sheets(self):
        return self._datasets

    def add_sheet(self, dataset):
        """Adds given :class:`Dataset` to the :class:`Databook`."""
        if isinstance(dataset, Dataset):
            self._datasets.append(dataset)
        else:
            raise InvalidDatasetType

    def _package(self, ordered=True):
        """Packages :class:`Databook` for delivery."""
        # pythonlibrary.net: 
        # 返回一个包含所有dataset的列表
        collector = []

        if ordered:
            dict_pack = OrderedDict
        else:
            dict_pack = dict

        for dset in self._datasets:
            collector.append(dict_pack(
                title=dset.title,
                data=dset._package(ordered=ordered)
            ))
        return collector

    @property
    def size(self):
        """The number of the :class:`Dataset` objects within :class:`Databook`."""
        return len(self._datasets)

    def load(self, in_stream, format, **kwargs):
        """
        Import `in_stream` to the :class:`Databook` object using the `format`.
        `in_stream` can be a file-like object, a string, or a bytestring.

        :param \\*\\*kwargs: (optional) custom configuration to the format `import_book`.
        """

        stream = normalize_input(in_stream)
        if not format:
            format = detect_format(stream)

        fmt = registry.get_format(format)
        if not hasattr(fmt, 'import_book'):
            # pythonlibrary.net: 
            # 格式处理器主要具有import_book
            raise UnsupportedFormat('Format {} cannot be loaded.'.format(format))

        fmt.import_book(self, stream, **kwargs)
        return self

    def export(self, format, **kwargs):
        """
        Export :class:`Databook` object to `format`.

        :param \\*\\*kwargs: (optional) custom configuration to the format `export_book`.
        """
        fmt = registry.get_format(format)
        if not hasattr(fmt, 'export_book'):
            # pythonlibrary.net: 
            # 格式处理器主要具有export_book 
            raise UnsupportedFormat('Format {} cannot be exported.'.format(format))

        return fmt.export_book(self, **kwargs)


def detect_format(stream):
    """Return format name of given stream (file-like object, string, or bytestring)."""
    stream = normalize_input(stream)
    fmt_title = None
    for fmt in registry.formats():
        # pythonlibrary.net: 
        # 使用所有的格式处理器来检测给定数据流的格式
        try:
            if fmt.detect(stream):
                fmt_title = fmt.title
                break
        except AttributeError:
            pass
        finally:
            if hasattr(stream, 'seek'):
                stream.seek(0)
    return fmt_title


def import_set(stream, format=None, **kwargs):
    """Return dataset of given stream (file-like object, string, or bytestring)."""

    return Dataset().load(normalize_input(stream), format, **kwargs)


def import_book(stream, format=None, **kwargs):
    """Return dataset of given stream (file-like object, string, or bytestring)."""

    return Databook().load(normalize_input(stream), format, **kwargs)


registry.register_builtins()
