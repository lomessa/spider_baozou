#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2015-08-12 
# 描述：翻页抓取


import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

cur_path_ = os.path.dirname(__file__)
sys.path.append(os.path.join(cur_path_,'../'))


from bs4 import BeautifulSoup
import re
import time
import json
import traceback
import urllib2
import logging
import urlparse
import logging.handlers
import logging.config

from libs import errcode
from fetcher import Fetcher
from libs import utils
from database import BzHandler


def init_logger(site_name):
    log_file = os.path.join(cur_path_, '../logs/%s.log' % site_name)
    handler = logging.handlers.RotatingFileHandler(log_file,
                    maxBytes = 500*1024*1024, backupCount = 3)
    fmt = ("%(name)s %(levelname)s %(filename)s:%(lineno)s %(asctime)s"
            " %(process)d:%(thread)d  %(message)s")
    handler.setFormatter(logging.Formatter(fmt))
    logger = logging.getLogger(site_name)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


class HandlerBase(object):

    def __init__(self,**kargs):
        self.site_name = kargs.get("site_name","base")
        self.logger = init_logger(self.site_name)
        self.fetcher = Fetcher(self.logger)
        self.db = kargs.get("db",None)
        self.index_id = set()


    def fetch(self, task):
        url = task.get('url',"")
        try_times = task.get("try_times",3)
        result = {}
        if not url:
            result['errcode'] = errcode.INVALID_URL
            result['errmsg'] = 'invalid url'
            return task,result

        times = 0
        while True:
            if times >= try_times:break
            try:
                task,result = self.fetcher.fetch(task={'url':url})
                if result.get("status_code") < 400:
                    break
                else:
                    times += 1
                    continue
            except Exception as e:
                self.logger.warn("[500] %s %s" % (url, str(e)))
                times += 1
                continue
        return task, result


    def get_page_links(self,soup,requrl):
        """"
        return item list processed
        """
        return []


    def get_next_link(self,soup,requrl):
        """
        return next link that is should be crawled
        """
        return None


    def get_link_content(self,task):
        try:
            content = ""
            task,result = self.fetch(task)
            content = result['content']
            #tmp_content = utils.unzipData(content)
            return content
        except Exception as e:
            self.logger.warn("url[%s] get_link_content failed, errmsg=%s",task.get('url',''),str(e))
        return content


    def get_link_soup(self,task):
        try:
            content = self.get_link_content(task)
            if not content:return None
            soup = BeautifulSoup(content,"html.parser")
            return soup
        except Exception as e:
            self.logger.warn("url[%s] get_link_soup failed, errmsg=%s",task.get('url',''),str(e))
        return None


    def get_link_json(self,task):
        try:
            content = self.get_link_content(task)
            if not content:return None
            ret = json.loads(content)
            return ret
        except Exception as e:
            self.logger.warn("url[%s] get_link_json failed, errmsg=%s",task.get('url',''),str(e))
        return {}


    def process(self, item, requrl):
        pass
        return True


    def run(self, task):
        self.start_url,self.max_pages = task.get('url',''),task.get("max_pages",50)
        time_interval = task.get('time_interval',1)
        self.user_id = task.get('user_id',-1)

        if self.user_id not in [1000001,1000002,1000003,1000011,1000019,1000034]:
            return

        requrl,cur_page = self.start_url,0
        while True:
            if not requrl:break
            cur_page += 1
            if cur_page > self.max_pages and self.max_pages > 0:break 
            soup = self.get_link_soup({'url':requrl})
            if not soup:break
            try:
                link_list = self.get_page_links(soup, requrl)
                if link_list:
                    for item in link_list:
                        self.process(item,requrl)
                else:
                    self.logger.info("url[%s] has no next page", requrl)
                    break
            except Exception as e:
                #self.logger.warn(traceback.format_exc().replace("\n",""))
                self.logger.warn("url[%s] process failed, errmsg=%s" % (requrl,str(e)))
            nextlink = self.get_next_link(soup, requrl)
            if not nextlink:
                self.logger.info("url[%s] has no next page", requrl)
                break
            else:
                requrl = nextlink
            time.sleep(time_interval)
        self.logger.info("start_url[%s], get [%d] pages",self.start_url,cur_page)





if __name__ == "__main__":
    inst = HandlerBase()
    inst.run({'url':'http://i.youku.com/i/UNTY5MDQ5Njcy/videos?order=1&page=1'})
