#! /usr/bin/python
# -*- coding:UTF-8 -*-

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import os
import re
import urllib2
import urlparse
import traceback
import gzip
import zlib
import json
import time
import urllib
from urllib import unquote
import hashlib
import _mysql
import cPickle


def timestamp2str(timestamp,format_="%Y-%m-%d %H:%M:%S"):
    timeArray = time.localtime(timestamp)
    time_str = time.strftime(format_, timeArray)
    return time_str


def str2timestamp(time_str):
    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArray))
    return timestamp


def zipData(content,):
    import StringIO, gzip, zlib
    zbuf = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', compresslevel=5, fileobj=zbuf)
    zfile.write(content)
    zfile.close()
    return zbuf


def unzipData(zipped_data):
    import StringIO, gzip, zlib
    uzfile = gzip.GzipFile(mode='rb', fileobj=StringIO.StringIO(zipped_data))
    content = uzfile.read()
    uzfile.close()
    return content


def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


# 压缩网页内容 gzip压缩->转换成二进制
def compress(content_):
    content = zipData(content_).getvalue()
    #content = cPickle.dumps(content, 2)   
    return content


# 解压缩网页内容 二进制-> 对象 -> unzip
def decompress(blob):
    #print "blob",blob
    #content = cPickle.loads(blob)
    content = unzipData(blob)
    return content


def is_valid_url(url):
    if -1 != url.find('javascript'): return False
    if 0 == len(url): return False
    if './' == url: return False
    d = {}
    d['url'] = url
    try:
        json.dumps(d)
        return True
    except:
        return False
    return True


def get_host(url):
    head = 'http://'
    if not url.startswith(head):
        return None
    host = url.replace(head, '')
    return host.split('/')[0].split(':')[0]


def get_hostname_from_url(url):
    if 'http:' not in url:
        url = 'http://%s' %(url)
    proto, rest = urllib.splittype(url)
    host, rest = urllib.splithost(rest)
    host, port = urllib.splitport(host)
    return host


def url_md5(url):
    if not url.endswith('/'):
        url = url + '/'
    return long(hashlib.md5(url).hexdigest()[:16], 16) & long('7fffffffffffffff', 16)
 

def filter_str(input):
    return _mysql.escape_string(input)


def positive_int(input):
    ret = int(input)
    if ret < 0:
        sys.stderr.write('find negative number: %d\n' % ret)
        return 0
    return abs(ret)


def zippage(content):
    import StringIO, gzip, zlib
    buf = StringIO.StringIO()
    zfile = gzip.GzipFile(mode='wb', compresslevel=1, fileobj=zbuf)
    zfile.write(content)
    zfile.close()
    return zbuf


def unzippage(zipped_data):
    import StringIO, gzip, zlib
    uzfile = gzip.GzipFile(mode='rb', fileobj=StringIO.StringIO(zipped_data))
    content = uzfile.read()
    uzfile.close()
    return content


def getcharset(html ,content_type):
    decode = 0
    header_match = re.search(r'<meta(.*?)charset="(?P<charset>.*?)">', html)
    if header_match:
        return header_match.group('charset')

    header_match = re.search(r'<meta(.*?)charset=("?)(?P<charset>.*?)"(\s*?)/>', html)
    if header_match:
        return header_match.group('charset')

    header_match = re.search('^(.*?)charset=(?P<charset>.*)', html)
    if header_match:
        return header_match.group('charset')

    header_match = re.search(r'charset=(?P<charset>.*)"', html)
    if header_match:
        return header_match.group('charset')


def http_load(url ,ref):
    time = 0
    while time < 3:
        try:
            req = urllib2.Request(url, None, {'User-Agent':'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; .NET CLR 2.0.50727; MS-RTC LM 8)','Referer': ref})
            opener = urllib2.urlopen(req,timeout=5)
            return opener.read()
        except Exception, e:
            print('open url:%s with refer:%s failed!error %s' % (url, ref, str(e)))
            sleep(1)
            time += 1
            continue
    return None


def _datetime_from_str(time_str):
    """Return (<scope>, <datetime.datetime() instance>) for the given
    datetime string.
    
    >>> _datetime_from_str("2009")
    ('year', datetime.datetime(2009, 1, 1, 0, 0))
    >>> _datetime_from_str("2009-12")
    ('month', datetime.datetime(2009, 12, 1, 0, 0))
    >>> _datetime_from_str("2009-12-25")
    ('day', datetime.datetime(2009, 12, 25, 0, 0))
    >>> _datetime_from_str("2009-12-25 13")
    ('hour', datetime.datetime(2009, 12, 25, 13, 0))
    >>> _datetime_from_str("2009-12-25 13:05")
    ('minute', datetime.datetime(2009, 12, 25, 13, 5))
    >>> _datetime_from_str("2009-12-25 13:05:14")
    ('second', datetime.datetime(2009, 12, 25, 13, 5, 14))
    >>> _datetime_from_str("2009-12-25 13:05:14.453728")
    ('microsecond', datetime.datetime(2009, 12, 25, 13, 5, 14, 453728))
    """
    import time
    import datetime
    formats = [
        # <scope>, <pattern>, <format>
        ("year", "YYYY", "%Y"),
        ("month", "YYYY-MM", "%Y-%m"),
        ("day", "YYYY-MM-DD", "%Y-%m-%d"),
        ("hour", "YYYY-MM-DD HH", "%Y-%m-%d %H"),
        ("minute", "YYYY-MM-DD HH:MM", "%Y-%m-%d %H:%M"),
        ("second", "YYYY-MM-DD HH:MM:SS", "%Y-%m-%d %H:%M:%S"),
        # ".<microsecond>" at end is manually handled below
        ("microsecond", "YYYY-MM-DD HH:MM:SS", "%Y-%m-%d %H:%M:%S"),
    ]
    for scope, pattern, format in formats:
        if scope == "microsecond":
            # Special handling for microsecond part. AFAIK there isn't a
            # strftime code for this.
            if time_str.count('.') != 1:
                continue
            time_str, microseconds_str = time_str.split('.')
            try:
                microsecond = int((microseconds_str + '000000')[:6])
            except ValueError:
                continue
        try:
            # This comment here is the modern way. The subsequent two
            # lines are for Python 2.4 support.
            #t = datetime.datetime.strptime(time_str, format)
            t_tuple = time.strptime(time_str, format)
            t = datetime.datetime(*t_tuple[:6])
        except ValueError:
            pass
        else:
            if scope == "microsecond":
                t = t.replace(microsecond=microsecond)
            return scope, t
    else:
        raise ValueError("could not determine date from %r: does not "
            "match any of the accepted patterns ('%s')"
            % (time_str, "', '".join(s for s,p,f in formats)))


def eq(a, b):
    return a == b

def gt(a, b):
    return a > b

def ge(a, b):
    return a >= b

def lt(a, b):
    return a < b

def le(a, b):
    return a <= b

def calc(type, a, b):
    calculation = {
        'eq' : lambda:eq(a, b),
        'gt' : lambda:gt(a, b),
        'ge' : lambda:ge(a, b),
        'lt' : lambda:lt(a, b),
        'le' : lambda:le(a, b)
    }
    return calculation[type]()


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
        return True
    else:
        return False


def is_number(uchar):
    """判断一个unicode是否是数字"""
    if uchar >= u'\u0030' and uchar<=u'\u0039':
        return True
    else:
        return False


def is_alphabet(uchar):
    """判断一个unicode是否是英文字母"""
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
        return True
    else:
        return False

def is_other(uchar):
    """判断是否非汉字，数字和英文字符"""
    if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
        return True
    else:
        return False