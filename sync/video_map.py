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
import base64
import random
import json
from   decimal import Decimal
from   operator import itemgetter
from   exceptions import Exception
import hashlib   

cur_path_ = os.path.dirname(__file__)
sys.path.append(os.path.join(cur_path_,'../'))

from libs import utils
from database   import BzHandler,BzRemoteHandler
from video_hash import VideoHash


class VideoMapper(object):

    def __init__(self,logger):
        pass
        self.logger = logger
        self.local_db = BzHandler()
        self.video_hash_ = VideoHash(logger)  #


    def get_video_list_from_db(self,bz_user_id,max_video_id):
        video_list = []
        for video in self.local_db.get_all_video_info(bz_user_id,max_video_id):
            video['video_sign'] = ''
            if video['video_upload_time']:
                video['video_upload_time'] = int(time.mktime(video['video_upload_time'].timetuple()))
            else:
                video['video_upload_time'] = 0
            video_list.append(video)
        self.logger.info('get videos from db ok, len=%d' % len(video_list))
        return video_list


    def get_video_hash_from_local(self,video_json_file):
        if not os.path.exists(video_json_file):
            return {}
        try:
            video_num = 0
            video_time_hash = json.loads(open(video_json_file).read())
            for k, video_list in video_time_hash.items():
                video_num += len(video_list)
            self.logger.info('load video hash ok, len=%d' % video_num)
            return video_time_hash
        except Exception as e:
            self.logger.warn("load video [%s] %s" % (video_json_file,str(e)))
        return {}


    def dump_video_hash(self,video_time_hash,video_json_file):
        try:
            json.dump(video_time_hash,open(video_json_file,'w'),
                ensure_ascii=False,indent=2)
            return True
        except Exception as e:
            self.logger.warn("dumps video hash [%s] %s" % (video_json_file,str(e)))
        return False


    def add_2_time_hash(self,video_list, video_time_hash):
        if not video_list:
            return video_time_hash
        for video in video_list:
            seconds = video.get('video_seconds',0)
            seconds = seconds / 5
            if seconds not in video_time_hash:
                video_time_hash[seconds] = []
            video_time_hash[seconds].append(video)
        return video_time_hash


    def get_video_sign_hash(self,video_time_hash):
        video_sign_hash = {}
        for vtime, video_list in video_time_hash.items():
            for video in video_list:
                video_id = video['video_id']
                if 'video_sign' in video and video['video_sign']:
                    video_sign_hash[video_id] = video['video_sign']
        return video_sign_hash


    def get_video_id_hash(self,video_time_hash):
        video_id_hash = {}
        for vtime, video_list in video_time_hash.items():
            for video in video_list:
                video_id = video['video_id']
                video_id_hash[video_id] = video
        return video_id_hash


    def get_max_video_id(self,video_time_hash):
        if not video_time_hash:
            return 0
        max_video_id = 0
        for k,video_list in video_time_hash.items():
            for video in video_list:
                if video['video_id'] > max_video_id:
                    max_video_id = video['video_id']
        return max_video_id


    def sign(self,video):
        video_id = video['video_id']
        md5 = hashlib.md5()
        md5.update(str(video_id))
        return md5.hexdigest()


    def _map(self,bz_user_id,video_json_file):
        # video_time => video
        video_time_hash = self.get_video_hash_from_local(video_json_file)
        # video_id => video_sign
        video_sign_hash = self.get_video_sign_hash(video_time_hash)
       
        add_video_list = self.get_video_list_from_db(bz_user_id,
                                    self.get_max_video_id(video_time_hash))
        if add_video_list:
            video_time_hash = self.add_2_time_hash(add_video_list,video_time_hash)

        # video_id => video
        video_id_hash = self.get_video_id_hash(video_time_hash)

        self.logger.info('before map: total video %d, %d has signature' % (
                                            len(video_id_hash),len(video_sign_hash)))
        for vt, video_list in video_time_hash.items():
            if not vt:continue
            for idx_1, video_1 in enumerate(video_list):
                for video_2 in video_list[idx_1+1:]:
                    vid_1, vid_2 = video_1['video_id'], video_2['video_id']
                    sign_1, sign_2 = video_sign_hash.get(vid_1,''),video_sign_hash.get(vid_2,'')
                    if sign_1 and sign_2:continue
                    title_sim,seconds_sim,uptime_sim = self.video_hash_.similar(video_1,video_2)
                    if title_sim + seconds_sim > 1.6:
                        if not sign_1 and not sign_2:
                            sign_tmp = self.sign(video_1)
                            video_sign_hash[vid_1] = sign_tmp
                            video_sign_hash[vid_2] = sign_tmp
                            video_id_hash[vid_1]['video_sign'] = sign_tmp
                            video_id_hash[vid_2]['video_sign'] = sign_tmp
                        elif sign_1:
                            video_sign_hash[vid_2] = sign_1
                            video_id_hash[vid_2]['video_sign'] = sign_1
                        elif sign_2:
                            video_sign_hash[vid_1] = sign_2
                            video_id_hash[vid_1]['video_sign'] = sign_2

        self.logger.info('after map: total video %d, %d has signature' % (
                                            len(video_id_hash),len(video_sign_hash)))
        
        no_sign = 0
        for vid,video in video_id_hash.items():
            if 'video_sign' not in video or not video['video_sign']:
                no_sign += 1
                video['video_sign'] = self.sign(video)
        self.logger.info('after post: total video %d, %d complement' % (
                                            len(video_id_hash),no_sign))                
        self.dump_video_hash(video_time_hash,video_json_file)
        return video_time_hash

     
    def map(self,bz_user_id,video_json_file):
        video_time_hash = self._map(bz_user_id,video_json_file)
        ret = []
        sign_sim_hash = {}
        for vt, videos in video_time_hash.items():
            for video in videos:
                ret.append(video)
                vs = video['video_sign']
                if vs not in sign_sim_hash:
                    sign_sim_hash[vs] = []
                sign_sim_hash[vs].append(video)
        for s, videos in sign_sim_hash.items():
            print "*" * 60
            for video in videos:
                self.pprint_video(video)
        return ret


    def pprint_video(self,video):
        print "%-6s\t%-5s\t%-30s\t%-50s" % (video['site_name'],video['video_seconds'],
                                                video['video_title'],video['video_url'])
        #print video['url']


if __name__ == "__main__":
    import logging
    import logging.handlers
    import logging.config

    log_file = os.path.join(os.path.dirname(__file__), '../logs/test.log')
    handler = logging.handlers.RotatingFileHandler(log_file,
                                maxBytes = 500*1024*1024, backupCount = 3)
    fmt = ("%(name)s %(levelname)s %(filename)s:%(lineno)s %(asctime)s"
            " %(process)d:%(thread)d  %(message)s")
    handler.setFormatter(logging.Formatter(fmt))
    logger = logging.getLogger("test")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    inst  = VideoMapper(logger)
    inst.map('暴走漫画','../data/video_bzmh.json')