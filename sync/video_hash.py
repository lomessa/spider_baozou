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

cur_path_ = os.path.dirname(__file__)
sys.path.append(os.path.join(cur_path_,'../'))

#sys.path.append(os.path.join(cur_path_,'../nlp/trunk'))
#sys.path.append(os.path.join(cur_path_,'../nlp/trunk/nlp/lib/jieba'))

from libs import utils
from database import BzHandler


import re
import time
import math
import base64
import random
import json
from   decimal     import Decimal
from   operator    import itemgetter
from   exceptions  import Exception
import hashlib   
import jieba
import jieba.analyse
import nlp.dict
import nlp.common
import nlp.tagger
import nlp.wordseg
float_repr = lambda o: format(Decimal(o), '.4f')

class Tagger(object):

    def __init__(self, with_stop_word=True, with_user_dict=False, with_user_idf_dict=False):

        if with_stop_word:
            jieba.analyse.set_stop_words(nlp.dict.STOP_WORD_LIST)
        if with_user_dict:
            jieba.load_userdict(nlp.dict.USER_DICT)
        if with_user_idf_dict:
            jieba.analyse.set_idf_path(nlp.dict.USER_IDF_DICT)
        self._location = nlp.common.load_set_from_file(nlp.dict.LOCATION_LIST)

        self.allow_pos = []
        #self.allow_pos = ['n','t','s','f','v','z','b','a','r','m','q']


    def extract_tags(self, content, top_k=20):
        tags = nlp.tagger.extract_tags(content, top_k, 3,allow_pos=self.allow_pos)
        if not tags:
            return []
        for tag in tags:
            tagtype = 1 if tag[u'tag'] in self._location else 0
            tag[u'tagtype'] = tagtype
        return tags[:top_k]



class VideoHash(object):

    def __init__(self,logger):

        self.tagger = Tagger()
        self.logger = logger
        self.segger = nlp.wordseg.Wordseg()
        self.db = BzHandler()

        self.tag_cache_ = {}
        self.seg_cache_ = {}


    def tag(self, title):  
        if not title:
            return []

        if title in self.tag_cache_:
            return self.tag_cache_[title]
        tags = self.tagger.extract_tags(title)
        self.tag_cache_[title] = tags
        # 提取标签
        return tags


    def seg(self,title):
        if not title:
            return []
        if title in self.seg_cache_:
            return self.seg_cache_[title]
        words = self.segger.cut(title)
        self.seg_cache_[title] = words
        return words


    def hash_old(self, title, seconds, **kargs):

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


    def hash(self,title, seconds, **kargs):

        # 过滤不包含中文、英文字母、或者数字的tag
        def is_character(word):
            if not isinstance(word,unicode):
                word = word.decode("utf-8")
            for w in word:
                if not utils.is_other(w):
                    return True
            return False

        debug = kargs.get('debug',False)
        finger_print_arr = [0,]*8 #生成长度为8的，每个元素为0的数组

        if self.logger:self.logger.info('%s\t%s' % (title,seconds))

        title_tags = self.tag(title)  #得到关键词

        if debug:
            print json.dumps(title_tags,ensure_ascii=False)
        title_tags = [x['tag'] for x in title_tags if is_character(x['tag'])]

        seconds_tag = seconds/5
        tags = sorted(title_tags + [str(seconds_tag)])
        if debug:
            print json.dumps(tags,ensure_ascii=False)
        
        if self.logger:
            self.logger.info("%s\t%s\t%s" % (title,seconds,json.dumps(tags,ensure_ascii=False)))
        md5 = hashlib.md5()
        md5.update("\3".join(tags))
        return md5.hexdigest()


    def title_similar(self,video_1,video_2):

        # 过滤不包含中文、英文字母、或者数字的tag
        def is_character(word):
            if not isinstance(word,unicode):
                word = word.decode("utf-8")
            for w in word:
                if not utils.is_other(w):
                    return True
            return False


        def regexp_tag(regexpr,title,*group_num):
            if not isinstance(title,unicode):
                title = title.decode("utf-8")
            match_obj = re.search(regexpr,title)
            if not match_obj:return ['',] * len(group_num)
            ret = []
            for i in group_num:
                match_str = match_obj.group(i)
                ret.append(match_str)
            return ret


        if not video_1['video_title'] or not video_2['video_title']:
            return 0.0
        """
        words_1 = set(self.segger.cut(video_1['title']))
        words_2 = set(self.segger.cut(video_2['title']))
        if len(words_1) > len(words_2):
            words_1,words_2 = words_2,words_1
        min_len,max_len = len(words_1),len(words_2)
        s_hit,d_hit = 0,0
        for wd in words_1:
            s_hit = s_hit + 1 if wd in words_2 else s_hit
            d_hit = d_hit + 1 if wd in words_2 and wd.isdigit() else d_hit
        if min_len <= 0 or s_hit <= 0:return 0.0
        sim = s_hit*1.0/max_len
        if d_hit > 0:sim = sim*(1.1**d_hit)
        return sim if sim < 1.0 else 1.0
        """
        tags_1  = self.tag(video_1['video_title'])
        tags_2  = self.tag(video_2['video_title'])
        tags_1  = [x['tag'] for x in tags_1 if is_character(x['tag'])]
        tags_2  = [x['tag'] for x in tags_2 if is_character(x['tag'])]
        words_1 = self.seg(video_1['video_title'])
        words_2 = self.seg(video_2['video_title'])
        if not tags_1 or not tags_2:return 0.0

        en_1    = regexp_tag(ur".*?(\d+)\s*?$",video_1['video_title'],1)[0]
        en_2    = regexp_tag(ur".*?(\d+)\s*?$",video_2['video_title'],1)[0]
        mn_1    = regexp_tag(ur".*?[\(（【\[](\d+)[\)）】\]]\s*?$",video_1['video_title'],1)[0]
        mn_2    = regexp_tag(ur".*?[\(（【\[](\d+)[\)）】\]]\s*?$",video_2['video_title'],1)[0]
        has_n_1 = en_1 or mn_1
        has_n_2 = en_2 or mn_2
        n_1     = en_1 if en_1 else mn_1
        n_2     = en_2 if en_2 else mn_2

        min_tags_len = min(len(tags_1),len(tags_2))
        words_diff   = len((set(words_1) | set(words_2)) - (set(words_1) & set(words_2)))
        tags_sim     = len(set(tags_1) & set(tags_2))*1.0 / min_tags_len


        if has_n_1 and has_n_2:
            if words_diff == 2 and n_1 != n_2:
                return 0.1
        elif has_n_1 and not has_n_2:
            if n_1 not in set(words_2):
                return 0.1
        elif has_n_2 and not has_n_1:
            if n_2 not in set(words_1):
                return 0.1
        return tags_sim

        

        tag_sim = len(set(words_1) & set(words_2))*1.0 / min_len
        
        
        return tag_sim


    def similar(self,video_1,video_2):
        title_sim = self.title_similar(video_1,video_2)

        seconds_sim = 1.0
        if not video_1['video_seconds'] or not video_2['video_seconds']:
            seconds_sim = 0.0
        else:
            seconds_abs = abs(video_1['video_seconds']-video_2['video_seconds'])
            seconds_ratio = seconds_abs/max(video_1['video_seconds'],video_2['video_seconds'])
            seconds_sim = 1 - seconds_ratio

        uptime_sim = 1.0
        if not video_1['video_upload_time'] or not video_2['video_upload_time']:
            uptime_sim = 0.0
        else:
            uptime_abs = abs(video_1['video_upload_time'] - video_2['video_upload_time'])
            uptime_sim = 1.0/(1.2**(uptime_abs/86400))
        return title_sim,seconds_sim,uptime_sim


    def test(self):
        v_1 = {'title':'每日一暴合集88'}
        v_2 = {'title':'每日一暴合集98'}
        print self.title_similar(v_1,v_2)


if __name__ == "__main__":
    inst = VideoHash(None)
    # test_txt_1 = u"飞碟一分钟】一分钟告诉你晒伤怎么办"
    # test_txt_2 = u"【飞碟一分钟】一分钟告诉你崴了脚怎么办"
    # print inst.hash(test_txt_1,88,debug=True)
    # print inst.hash(test_txt_2,88,debug=True)

    # print json.dumps(inst.seg(test_txt_1,),ensure_ascii=False)
    # print json.dumps(inst.seg(test_txt_2,),ensure_ascii=False)
    inst.test()
    #print (len(set(test_txt_1) & set(test_txt_2)) + 0.0) / max(len(test_txt_1),len(test_txt_2))
    #print inst.similarity(inst.hash(test_txt_1,171),inst.hash(test_txt_2,121))



