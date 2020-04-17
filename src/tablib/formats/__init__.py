""" Tablib - formats
"""
from collections import OrderedDict
from functools import partialmethod
from importlib import import_module
from importlib.util import find_spec

from tablib.exceptions import UnsupportedFormat
from tablib.utils import normalize_input

from ._csv import CSVFormat
from ._json import JSONFormat
from ._tsv import TSVFormat

uninstalled_format_messages = {
    "cli": {"package_name": "tabulate package", "extras_name": "cli"},
    "df": {"package_name": "pandas package", "extras_name": "pandas"},
    "html": {"package_name": "MarkupPy package", "extras_name": "html"},
    "ods": {"package_name": "odfpy package", "extras_name": "ods"},
    "xls": {"package_name": "xlrd and xlwt packages", "extras_name": "xls"},
    "xlsx": {"package_name": "openpyxl package", "extras_name": "xlsx"},
    "yaml": {"package_name": "pyyaml package", "extras_name": "yaml"},
}


def load_format_class(dotted_path):
    try:
        # pythonlibrary.net: 
        # rsplit('.', 1) 会仅仅从右边使用.符号对字符串进行一次分拆
        module_path, class_name = dotted_path.rsplit('.', 1)
        # pythonlibrary.net: 
        # import_module(module_path) 将会导入module_path路径下的模块，并返回该模块
        # 通过getattr函数可以获取到模块下的类
        # 例如：
        # import_module('os')  将导入模块 os
        # getattr(import_module('os'), 'path')  将获取到 os.path
        # 使用这种方法可以动态的导入模块
        return getattr(import_module(module_path), class_name)
    except (ValueError, AttributeError) as err:
        raise ImportError("Unable to load format class '{}' ({})".format(dotted_path, err))


class FormatDescriptorBase:
    def __init__(self, key, format_or_path):
        self.key = key
        self._format_path = None
        if isinstance(format_or_path, str):
            self._format = None
            self._format_path = format_or_path
        else:
            self._format = format_or_path

    def ensure_format_loaded(self):
        # pythonlibrary.net: 
        # _format 是从模块中加载进来的类
        if self._format is None:
            self._format = load_format_class(self._format_path)


class ImportExportBookDescriptor(FormatDescriptorBase):
    def __get__(self, obj, cls, **kwargs):
        # pythonlibrary.net: 
        # 描述器
        # obj是parent类的实例
        # cls是parent类
        self.ensure_format_loaded()
        return self._format.export_book(obj, **kwargs)

    def __set__(self, obj, val):
        self.ensure_format_loaded()
        return self._format.import_book(obj, normalize_input(val))


class ImportExportSetDescriptor(FormatDescriptorBase):
    def __get__(self, obj, cls, **kwargs):
        self.ensure_format_loaded()
        return self._format.export_set(obj, **kwargs)

    def __set__(self, obj, val):
        self.ensure_format_loaded()
        return self._format.import_set(obj, normalize_input(val))


class Registry:
    _formats = OrderedDict()

    def register(self, key, format_or_path):
        from tablib.core import Databook, Dataset

        # Create Databook.<format> read or read/write properties

        # pythonlibrary.net: 
        # 下边这些代码会直接修改到Dataset类和Databook类的类属性
        # 它们将会添加新的属性到这两个类，而新的属性就是前边的格式描述器
        # 描述器具有一个成员名为_format，描述器的setter和getter将会
        # 调用_format的import和export
        setattr(Databook, key, ImportExportBookDescriptor(key, format_or_path))

        # Create Dataset.<format> read or read/write properties,
        # and Dataset.get_<format>/set_<format> methods.
        setattr(Dataset, key, ImportExportSetDescriptor(key, format_or_path))
        try:
            setattr(Dataset, 'get_%s' % key, partialmethod(Dataset._get_in_format, key))
            setattr(Dataset, 'set_%s' % key, partialmethod(Dataset._set_in_format, key))
        except AttributeError:
            setattr(Dataset, 'get_%s' % key, partialmethod(Dataset._get_in_format, key))

        self._formats[key] = format_or_path

    def register_builtins(self):
        # Registration ordering matters for autodetection.
        self.register('json', JSONFormat())
        # xlsx before as xls (xlrd) can also read xlsx

        # pythonlibrary.net: 
        # find_spec函数可以检测某一个python模块是不是被安装
        # 只注册那些依赖库被安装过的格式处理器
        # 因为JSONFormat, CSVFormat, TSVFrmat不需要第三方依赖库，所以可以以类实例的方法直接注册
        # 其他格式处理器都需要安装依赖，因此，先要判断是否安装，如果安装了，则通过类本身注册
        if find_spec('openpyxl'):
            self.register('xlsx', 'tablib.formats._xlsx.XLSXFormat')
        if find_spec('xlrd') and find_spec('xlwt'):
            self.register('xls', 'tablib.formats._xls.XLSFormat')
        if find_spec('yaml'):
            self.register('yaml', 'tablib.formats._yaml.YAMLFormat')
        self.register('csv', CSVFormat())
        self.register('tsv', TSVFormat())
        if find_spec('odf'):
            self.register('ods', 'tablib.formats._ods.ODSFormat')
        self.register('dbf', 'tablib.formats._dbf.DBFFormat')
        if find_spec('MarkupPy'):
            self.register('html', 'tablib.formats._html.HTMLFormat')
        self.register('jira', 'tablib.formats._jira.JIRAFormat')
        self.register('latex', 'tablib.formats._latex.LATEXFormat')
        if find_spec('pandas'):
            self.register('df', 'tablib.formats._df.DataFrameFormat')
        self.register('rst', 'tablib.formats._rst.ReSTFormat')
        if find_spec('tabulate'):
            self.register('cli', 'tablib.formats._cli.CLIFormat')

    def formats(self):
        # pythonlibrary.net: 
        # 所有的格式处理器将被放在_formats
        for key, frm in self._formats.items():
            if isinstance(frm, str):
                self._formats[key] = load_format_class(frm)
            yield self._formats[key]

    def get_format(self, key):
        # pythonlibrary.net: 
        # 通过格式处理器的名字获取格式处理器
        if key not in self._formats:
            if key in uninstalled_format_messages:
                raise UnsupportedFormat(
                    "The '{key}' format is not available. You may want to install the "
                    "{package_name} (or `pip install tablib[{extras_name}]`).".format(
                        **uninstalled_format_messages[key], key=key
                    )
                )
            raise UnsupportedFormat("Tablib has no format '%s' or it is not registered." % key)
        if isinstance(self._formats[key], str):
            self._formats[key] = load_format_class(self._formats[key])
        return self._formats[key]


registry = Registry()
