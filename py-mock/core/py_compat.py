#!/usr/bin/env python
# -*- coding: utf-8 -*-
# py_compat.py
#

from __future__ import unicode_literals
from __future__ import print_function, absolute_import, division

import sys

PY3 = sys.version_info.major >= 3
PY2 = sys.version_info.major == 2

if PY2:
    from urlparse import urlparse
    from urllib import quote as urlquote, unquote as urlunquote

    builtin_str = str
    bytes = str
    # str = unicode

    reload(sys)
    sys.setdefaultencoding('utf-8')


    def to_bytes(data):
        """若输入为unicode， 则转为utf-8编码的bytes；其他则原样返回。"""
        if isinstance(data, unicode):
            return data.encode('utf-8')
        else:
            return data


    def to_string(data):
        return to_unicode(data)


    def to_unicode(data):
        if isinstance(data, bytes):
            return data.decode('utf-8')
        else:
            return data


    def stringify(input):
        if isinstance(input, dict):
            return dict([(stringify(key), stringify(value)) for key, value in input.iteritems()])
        elif isinstance(input, list):
            return [stringify(element) for element in input]
        elif isinstance(input, bytes):
            return to_unicode(input)
        else:
            return input

elif PY3:
    from importlib import reload
    from urllib.parse import urlparse
    from urllib.parse import quote as urlquote, unquote as urlunquote

    builtin_str = str
    bytes = bytes
    str = str
    unicode = str


    def to_bytes(data):
        """若输入为str（即unicode），则转为utf-8编码的bytes；其他则原样返回"""
        if isinstance(data, str):
            return data.encode(encoding='utf-8')
        else:
            return data


    def to_string(data):
        """若输入为bytes，则认为是utf-8编码，并返回str"""
        if isinstance(data, bytes):
            return data.decode('utf-8')
        else:
            return data


    def to_unicode(data):
        return to_string(data)


    def stringify(input):
        if isinstance(input, bytes):
            return to_string(input)
        else:
            return input
