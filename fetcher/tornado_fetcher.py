#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2012-12-17 11:07:19

import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import json
import pprint
import Queue
import logging
import threading
import cookie_utils
import tornado.ioloop
import tornado.httputil
import tornado.httpclient
#from tornado.curl_httpclient import CurlAsyncHTTPClient
#from tornado.curl_httpclient import AsyncHTTPClient
#from tornado.simple_httpclient import SimpleAsyncHTTPClient


allowed_options = ['method', 'data', 'timeout', 'allow_redirects', 'cookies']

def text(obj, encoding='utf-8'):
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    return obj


def unicode_obj(obj, encoding='utf-8'):
    if isinstance(obj, str):
        return obj.decode(encoding)
    return obj


class Fetcher(object):

    user_agent = ("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/43.0.2357.130 Safari/537.36")
    default_options = {'method': 'GET','headers': {},'timeout': 120,}


    # 初始化
    def __init__(self, logger, poolsize=100, proxy=None, async=False):

        self.poolsize = poolsize
        self.proxy = proxy
        self.async = async
        self.ioloop = tornado.ioloop.IOLoop()
        # binding io_loop to http_client here
        if self.async:
            self.http_client = AsyncHTTPClient(max_clients=self.poolsize,io_loop=self.ioloop)
        else:
            self.http_client = tornado.httpclient.HTTPClient(max_clients=self.poolsize)

        self.logger = logger


    def send_result(self, type, task, result):
        """type in ('data', 'http')"""
        pass


    def fetch(self, task, callback=None):
        url = task.get('url', 'data:,')
        if callback is None:
            callback = self.send_result
        if url.startswith('data:'):
            return self.data_fetch(url, task, callback)
        else:
            return self.http_fetch(url, task, callback)

    def data_fetch(self, url, task, callback):
        result = {}
        result['orig_url'] = url
        result['content'] = dataurl.decode(url)
        result['headers'] = {}
        result['status_code'] = 200
        result['url'] = url
        result['cookies'] = {}
        result['time'] = 0
        result['save'] = task.get('fetch', {}).get('save')
        if len(result['content']) < 70:
            self.logger.info("[200] %s 0s", url)
        else:
            self.logger.info(
                "[200] data:,%s...[content:%d] 0s",
                result['content'][:70],
                len(result['content'])
            )

        callback('data', task, result)
        return task, result

    

    def http_fetch(self, url, task, callback):
        start_time = time.time()
        fetch = dict(self.default_options)
        fetch.setdefault('url', url)
        fetch.setdefault('headers', {})
        fetch.setdefault('allow_redirects', True)
        fetch.setdefault('use_gzip', True)
        fetch['headers'].setdefault('User-Agent', self.user_agent)

        task_fetch = task.get('fetch', {})
        for each in allowed_options:
            if each in task_fetch:
                fetch[each] = task_fetch[each]
        fetch['headers'].update(task_fetch.get('headers', {}))
        track_headers = task.get('track', {}).get('fetch', {}).get('headers', {})

        if 'proxy' in task_fetch:
            if isinstance(task_fetch['proxy'], basestring):
                fetch['proxy_host'] = task_fetch['proxy'].split(":")[0]
                fetch['proxy_port'] = int(task_fetch['proxy'].split(":")[1])
            elif self.proxy and task_fetch.get('proxy', True):
                fetch['proxy_host'] = self.proxy.split(":")[0]
                fetch['proxy_port'] = int(self.proxy.split(":")[1])

        if task_fetch.get('etag', True):
            _t = task_fetch.get('etag') if isinstance(task_fetch.get('etag'), basestring) \
                else track_headers.get('etag')
            if _t:
                fetch['headers'].setdefault('If-None-Match', _t)

        if task_fetch.get('last_modified', True):
            _t = task_fetch.get('last_modifed') \
                if isinstance(task_fetch.get('last_modifed'), basestring) \
                else track_headers.get('last-modified')
            if _t:
                fetch['headers'].setdefault('If-Modifed-Since', _t)

        # fix for tornado request obj
        if 'allow_redirects' in fetch:
            fetch['follow_redirects'] = fetch['allow_redirects']
            del fetch['allow_redirects']
        if 'timeout' in fetch:
            fetch['connect_timeout'] = fetch['timeout']
            fetch['request_timeout'] = fetch['timeout']
            del fetch['timeout']
        if 'data' in fetch:
            fetch['body'] = fetch['data']
            del fetch['data']

        cookie = None
        if 'cookies' in fetch:
            cookie = fetch['cookies']
            del fetch['cookies']

        def handle_response(response):
            if response.error and not isinstance(response.error, tornado.httpclient.HTTPError):
                return handle_error(response.error)

            response.headers = final_headers
            session.extract_cookies_to_jar(request, cookie_headers)
            result = {}
            result['orig_url'] = url
            result['content'] = response.body or ''
            result['headers'] = dict(response.headers)
            result['status_code'] = response.code
            result['url'] = response.effective_url or url
            result['cookies'] = session.to_dict()
            result['time'] = time.time() - start_time
            result['save'] = task_fetch.get('save')
            if 200 <= response.code < 300:
                self.logger.info("[%d] %s %.2fs", response.code, url, result['time'])
            else:
                self.logger.warning("[%d] %s %.2fs", response.code, url, result['time'])
            callback('http', task, result)
            return task, result

        def header_callback(line):
            line = line.strip()
            if line.startswith("HTTP/"):
                final_headers.clear()
                return
            if not line:return
            final_headers.parse_line(line)
            cookie_headers.parse_line(line)

        def handle_error(error):
            result = {
                'status_code': getattr(error, 'code', 599),
                'error': getattr(error, 'message', '%r' % error),
                'content': "",
                'time': time.time() - start_time,
                'orig_url': url,
                'url': url,
            }
            self.logger.error("[599] %s, %r %.2fs", url, error, result['time'])
            callback('http', task, result)
            return task, result

        session = cookie_utils.CookieSession()
        cookie_headers = tornado.httputil.HTTPHeaders()
        final_headers = tornado.httputil.HTTPHeaders()
        try:
            request = tornado.httpclient.HTTPRequest(header_callback=header_callback, **fetch)
            if cookie:
                session.update(cookie)
                if 'Cookie' in request.headers:
                    del request.headers['Cookie']
                request.headers['Cookie'] = session.get_cookie_header(request)
            if self.async:
                self.http_client.fetch(request, handle_response)
            else:
                try:
                    return handle_response(self.http_client.fetch(request))
                except Exception as e:
                    return handle_error(e)
        except tornado.httpclient.HTTPError as e:
            if e.response:
                return handle_response(e.response)
            else:
                return handle_error(e)
        except Exception as e:
            return handle_error(e)


    def size(self):
        return self.http_client.size()




if __name__ == '__main__':
    fetcher = Fetcher(async=True)
    url = 'http://www.baidu.com'
    task = {'url':url}
    ret = fetcher.fetch(task)
    print ret

