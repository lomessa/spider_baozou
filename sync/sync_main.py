 #! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2015-08-12 
# 描述：视频指纹模块


import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import time
import math
import json
from   exceptions import Exception

cur_path_ = os.path.dirname(__file__)
sys.path.append(os.path.join(cur_path_,'../'))

from   database import BzHandler,BzRemoteHandler
from   video_hash import VideoHash
from   video_map import VideoMapper

import logging
import logging.handlers
import logging.config

log_file = os.path.join(os.path.dirname(__file__), '../logs/sync.log')
handler  = logging.handlers.RotatingFileHandler(log_file,
                maxBytes = 500*1024*1024, backupCount = 3)
fmt = ("%(name)s %(levelname)s %(filename)s:%(lineno)s %(asctime)s"
        " %(process)d:%(thread)d  %(message)s")
handler.setFormatter(logging.Formatter(fmt))
logger = logging.getLogger("sync")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class Sync(object):

    def __init__(self):
        self.local_db    = BzHandler()          
        self.remote_db   = BzRemoteHandler()
        self.video_hash_ = VideoHash(logger) 
        self.video_map_  = VideoMapper(logger)

    def sync_video_bak(self,last = 4):
        num = 0;total = 0
        last_time = (int(time.time())/3600 - last)*3600
        for video in self.local_db.get_video_info(last_time):
            total += 1
            video_sign = video.get("video_sign","")
            if not video_sign:
                video_sign = self.video_hash_.hash(video['video_title'],video['video_seconds'])
                self.local_db.save_video_sign(video['video_id'],video_sign)
                video['video_sign'] = video_sign

            if self.remote_db.save_video_info(video):
                num += 1

        timeArray = time.localtime(last_time)
        last_time_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        logger.info("sync video [%s] total[%d] number[%d]" % (last_time_str,total,num))


    def sync_video(self,last = 4):
        sync_user = [('暴走漫画','../data/video_bzmh.json')]
        for bz_user_id, video_json_file in sync_user:
            num = 0
            video_list = self.video_map_.map(bz_user_id, video_json_file)
            for video in video_list:
                video_sign = video.get("video_sign","")
                #if not video_sign:continue
                self.local_db.save_video_sign(video['video_id'],video_sign)
                if self.remote_db.save_video_info(video):
                    num += 1
            logger.info("sync %s video total[%d]" % (bz_user_id,num))
        pass


    def sync_user(self,last = 4):
        num = 0
        last_time = (int(time.time())/3600 - last)*3600
        for user in self.local_db.get_user_info():
            if self.remote_db.save_user_info(user):
                num += 1

        timeArray = time.localtime(last_time)
        last_time_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        logger.info("sync user [%s] number[%d]" % (last_time_str,num))



    def sync_video_static(self,last=4):
        num = 0
        last_time = (int(time.time())/3600 - last)*3600

        # complement data ...
        #
        #
        for static in self.local_db.get_video_static(last_time):
            self.remote_db.save_video_static(static)
            num += 1

        timeArray = time.localtime(last_time)
        last_time_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        logger.info("sync video static [%s] number[%d]" % (last_time_str,num))


    def sync_user_static(self,last=4):
        num = 0
        last_time = (int(time.time())/3600 - last)*3600

        # complement data ...
        #
        #
        for static in self.local_db.get_user_static(last_time):
            self.remote_db.save_user_static(static)
            num += 1
        timeArray = time.localtime(last_time)
        last_time_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        logger.info("sync user static [%s] number[%d]" % (last_time_str,num))



    def refresh_video_sign(self,last=24*100):
        total,local_num,remote_num = 0,0,0
        last_time = (int(time.time())/3600 - last)*3600
        for video in self.local_db.get_video_info(last_time):
            total += 1
            video_sign = self.video_hash_.hash(video['video_title'],video['video_seconds'])
            if self.local_db.save_video_sign(video['video_id'],video_sign):
                local_num += 1
            if self.remote_db.save_video_sign(video['video_id'],video_sign):
                remote_num += 1
        logger.info("sync video sign [%d] local[%d] remote[%d]" % (total,local_num,remote_num))


    def sync(self):

        self.sync_video(72)
        self.sync_video_static(4)
        self.sync_user_static(4)
        self.sync_user()


    def dump_video_info(self):
        num = 0;total = 0
        video_list = []
        for video in self.local_db.get_all_video_info('暴走漫画'):
            item = {}
            item['video_id'] = video['video_id']
            item['site_name'] = video['site_name']
            item['url'] = video['video_url']
            item['title'] = video['video_title']
            item['seconds'] = video['video_seconds']
            
            item['upload_time'] = None
            if video['video_upload_time']:
                item['upload_time'] = int(time.mktime(video['video_upload_time'].timetuple()))
            video_list.append(item)
        json.dump(video_list,open("../data/video_bzmh.json",'w'),ensure_ascii=False,indent=2)



if __name__ == "__main__":

    inst = Sync()
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(inst.sync_video(72*7))
    elif len(sys.argv) == 2 and sys.argv[1] == 'refresh_sign':
        sys.exit(inst.refresh_video_sign())
    elif len(sys.argv) == 2 and sys.argv[1] == 'dump_video':
        sys.exit(inst.dump_video_info())
    elif len(sys.argv) == 2 and sys.argv[1] == 'sync_video':
        sys.exit(inst.sync_video())
    inst.sync()
