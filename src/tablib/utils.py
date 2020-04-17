from io import BytesIO, StringIO


def normalize_input(stream):
    """
    Accept either a str/bytes stream or a file-like object and always return a
    file-like object.
    """
    # pythonlibrary.net: 
    # 如果steam是一个字符串，转换为StringIO，如果是bytes转换为BytesIO
    # 转成IO Stream的原因是，几乎所有的依赖库都可以导入IO Steam进行初始化
    if isinstance(stream, str):
        return StringIO(stream)
    elif isinstance(stream, bytes):
        return BytesIO(stream)
    return stream
