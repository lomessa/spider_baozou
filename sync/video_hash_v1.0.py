#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2015-08-12 
# 描述：视频指纹模块


import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append(os.path.expanduser('~/nlp/trunk'))
sys.path.append(os.path.expanduser('~/nlp/trunk/nlp/lib/jieba'))

import time
import math
import base64
import random
import json
from decimal import Decimal
from operator import itemgetter
from exceptions import Exception

import hashlib   

float_repr = lambda o: format(Decimal(o), '.4f')

import jieba
import jieba.analyse
import nlp.dict
import nlp.common
import nlp.tagger
import nlp.wordseg as wordseg


cur_path = os.path.abspath(os.path.dirname(__file__))
sys.path.append(cur_path+'/../')

from database import BzHandler

class Tagger(object):

    def __init__(self, with_stop_word=True, with_user_dict=False, with_user_idf_dict=False):
        if with_stop_word:
            jieba.analyse.set_stop_words(nlp.dict.STOP_WORD_LIST)
        if with_user_dict:
            jieba.load_userdict(nlp.dict.USER_DICT)
        if with_user_idf_dict:
            jieba.analyse.set_idf_path(nlp.dict.USER_IDF_DICT)
        self._location = nlp.common.load_set_from_file(nlp.dict.LOCATION_LIST)


    def extract_tags(self, content, top_k=20):
        tags = nlp.tagger.extract_tags(content, top_k, 3)
        if not tags:
            return []
        for tag in tags:
            tagtype = 1 if tag[u'tag'] in self._location else 0
            tag[u'tagtype'] = tagtype
        return tags[:top_k]


class VideoHash(object):

    def __init__(self,logger):
        self.seg = wordseg.Wordseg()
        self.tagger = None
        self.logger = logger
        self.db = BzHandler()


    def tag(self, title):
        if not title:
            return []
        if not self.tagger:
            self.tagger = Tagger()
        tags = self.tagger.extract_tags(title)
        if self.logger:
            self.logger.info(json.dumps(tags))
        # 提取标签
        return tags


    def hash_1(self, video):

        title = video['video_title']
        seconds = video['video_seconds']
        # 1. 提取标签
        self.tag(title)
        title_hash_ = [0]*16
        if isinstance(title,unicode):
            title = title.encode("utf-8")
        for ch in title:
            bits = "0"*(8-len(bin(ord(ch))[2:])) + bin(ord(ch))[2:]
            title_hash_[int(bits[:4],2)] += 1
            title_hash_[int(bits[4:],2)] += 1
        #seconds_hash = '0'*(32-len(bin(seconds)[2:]) + bin(seconds)[2:]
        return base64.b64encode("\3".join([str(x) for x in title_hash_ + [seconds]]))


    def hash_2(self,video, **kargs):
        title,seconds = video['video_title'],video['video_seconds']
        if self.logger:self.logger.info('%s\t%s' % (title,seconds))
        title_tags = [x['tag'] for x in self.tag(title)]
        seconds_tag = seconds/5
        tags = sorted(title_tags + [str(seconds_tag)])
        print json.dumps(tags,ensure_ascii=False)
        if self.logger:self.logger.info("%s\t%s\t%s" % (title,seconds,json.dumps(tags,ensure_ascii=False)))
        md5 = hashlib.md5()
        md5.update("\3".join(tags))
        return md5.hexdigest()


    def similar_1(self,video_1,video_2):
        title_1 = video_1['video_title']
        title_2 = video_2['video_title']
        seconds_abs = abs(video_1['video_seconds']-video_2['video_seconds'])
        sim_origin = (0.0+len(set(title_1) & set(title_2)))/(max(len(title_1),len(title_2)))                    
        sim = sim_origin/(1.1**seconds_abs)
        return sim


    def similar_2(self,video_1,video_2):
        if not video_1['video_title'] or not video_2['video_title']:
            return 0.0

        seg = self.seg
        words_1 = seg.cut(video_1['video_title'])
        words_2 = seg.cut(video_2['video_title'])
        # print json.dumps(words_1,ensure_ascii=False)
        # print json.dumps(words_2,ensure_ascii=False)

        if len(words_1) > len(words_2):
            words_1,words_2 = words_2,words_1

        max_len = max(len(words_1),len(words_2))
        min_len = min(len(words_1),len(words_2))

        s_hit,d_hit = 0,0
        for wd in words_1:
            s_hit = s_hit + 1 if wd in words_2 else s_hit
            d_hit = d_hit + 1 if wd in words_2 and wd.isdigit() else d_hit

        # print "min len:%d, max len:%d, s_hit:%d, d_hit:%d" % (len(words_1),len(words_2),s_hit,d_hit)
        if min_len <= 0 or s_hit <= 0:return 0.0

        sim = s_hit*1.0/max_len
        if d_hit > 0:sim = sim*(1.2**d_hit)
        return sim if sim < 1.0 else 1.0
        
    def similar_3(self,video_1,video_2):
        title_1 = video_1['video_title']
        title_2 = video_2['video_title']
        seconds_abs = abs(video_1['video_seconds']-video_2['video_seconds'])
        seconds_ratio = seconds_abs/max(video_1['video_seconds'],video_2['video_seconds'])
        sim_seconds = 1 - seconds_ratio
        title_1 = video_1['video_title']
        title_2 = video_2['video_title']
        sim_title = (0.0+len(set(title_1) & set(title_2)))/(max(len(title_1),len(title_2)))
        time_stamp_1 = int(time.mktime(video_1['video_upload_time'].timetuple())) if video_1['video_upload_time'] else 0
        time_stamp_2 = int(time.mktime(video_2['video_upload_time'].timetuple())) if video_2['video_upload_time'] else 0
        uploadTime_abs = abs(time_stamp_1-time_stamp_2)/3600
        if uploadTime_abs <= 24:
            sim_upload = 1 - uploadTime_abs / 24
        else:
            sim_upload = 0
        sim= sim_seconds * 0.6 + sim_title * 0.2 + sim_upload * 0.2
        return sim


    def test(self):
        videos = self.db.get_video_info(int(time.time()) - 86400*100)
        video_seconds_hash = {}
        for video in videos:
            seconds = video['video_seconds']
            seconds = seconds/5
            if seconds not in video_seconds_hash:
                video_seconds_hash[seconds] = []
            video_seconds_hash[seconds].append(video)

        for seconds, videos in video_seconds_hash.items():
            for idx_1, video_1 in enumerate(videos):
                for idx_2, video_2 in enumerate(videos[idx_1+1:]):
                    sim = self.similar_3(video_1,video_2) * 0.4 + self.similar_2(video_1,video_2) * 0.6
                    if sim > 0.70:
                        print "*" * 50
                        print "sim:%s" % sim
                        print "%s\t%s\t%s\t%s" % (video_1['video_title'],video_1['video_seconds'],str(video_1['video_upload_time']),video_1['video_url'])
                        print "%s\t%s\t%s\t%s" % (video_2['video_title'],video_2['video_seconds'],str(video_2['video_upload_time']),video_2['video_url'])


if __name__ == "__main__":
    inst = VideoHash(None)
    inst.test()
    # print inst.similar_3({'video_title':'每日一暴 ：究竟谁是“成功之父”？','video_seconds':85},{'video_title':'694 究竟谁是“成功之父”？','video_seconds':85})
    # print json.dumps(inst.tag("10个动漫迷都该知道的细节21【脑残师兄】搞笑"),ensure_ascii=False)


