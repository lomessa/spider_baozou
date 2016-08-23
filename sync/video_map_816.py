#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:zihe,wnehui
# 时间：2015-08-12 ,修改：2016-8-16
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
        self.video_hash_ = VideoHash(logger)  


    def get_video_list_from_db(self,bz_user_id,max_video_id):  #从本地数据库取出数据，并对时间标签进行处理
        video_list = []
        for video in self.local_db.get_all_video_info(bz_user_id,max_video_id):
            video['video_sign'] = ''
            if video['video_upload_time']:
                video['video_upload_time'] = int(time.mktime(video['video_upload_time'].timetuple()))  #将时间转化为int型
            else:
                video['video_upload_time'] = 0
            video_list.append(video)
        self.logger.info('get videos from db ok, len=%d' % len(video_list))
        return video_list


    def get_video_hash_from_local(self,video_json_file):  #
        if not os.path.exists(video_json_file):
            return {}
        try:
            video_num = 0
            video_time_hash = json.loads(open(video_json_file).read())
            for k, video_list in video_time_hash.items():
                for video in video_list:
                    if video['video_sign']:
                        video['video_sign'] = ''
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


    def add_2_time_hash(self,video_list, video_time_hash):  #更新时间hash
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
                    video_sign_hash[video_id] = video['video_sign']  #id -> video_sign
        return video_sign_hash


    def get_video_id_hash(self,video_time_hash):
        video_id_hash = {}
        for vtime, video_list in video_time_hash.items():
            for video in video_list:
                video_id = video['video_id']
                video_id_hash[video_id] = video
        return video_id_hash


    def get_max_video_id(self,video_time_hash):  #获得最大的video hash
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
        video_time_hash = self.get_video_hash_from_local(video_json_file)   #得到时间hash
        # video_id => video_sign
        video_sign_hash = self.get_video_sign_hash(video_time_hash)   
       
        add_video_list = self.get_video_list_from_db(bz_user_id,
                                    self.get_max_video_id(video_time_hash))
        if add_video_list:
            video_time_hash = self.add_2_：time_hash(add_video_list,video_time_hash)

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
   
  #求得规模大小->数量的哈希表

    def get_scale_num_hash(self,sign_sim_hash):
        scale_num_hash = {}

        for s,vs in sign_sim_hash.items():

            videos = vs
            scale = len(vs)
            if scale_num_hash.has_key(str(scale)):
                scale_num_hash[str(scale)]+=1
            else:
                scale_num_hash[str(scale)]=1
        return scale_num_hash

    def map(self,bz_user_id,video_json_file):
        video_time_hash = self._map(bz_user_id,video_json_file)
        ret = []
        sign_sim_hash = {}
        scale_num_hash_before = {}#召回前：规模大小->数量
        scale_num_hash_after = {}#召回后: 规模大小->数量
        for vt, videos in video_time_hash.items():
            for video in videos:
                ret.append(video)
                vs = video['video_sign']
                if vs not in sign_sim_hash:
                    sign_sim_hash[vs] = [] 
                sign_sim_hash[vs].append(video)
        scale_num_hash_before = self.get_scale_num_hash(sign_sim_hash) #求召回前的 规模->数量 哈希
        for scale,num in scale_num_hash_before.items():
            self.logger.info('befor recall:**********************the %d scale group has %d member*********************' %(int(scale),num))
        sign_sim_hash =  self.recall_single_2_group(sign_sim_hash)
        scale_num_hash_after =self.get_scale_num_hash(sign_sim_hash)  #求召回后的 规模->数量 哈希
        for scale,num in scale_num_hash_after.items():
            self.logger.info('after recall*********************the %d scale group has %d member*********************' %(int(scale),num))     
        for s, videos in sign_sim_hash.items():
            print "*" * 60
            for video in videos:
                self.pprint_video(video)
        return ret
       # sign_sim_hash = self.recall(sign_sim_hash)

   

    def recall_single_2_group(self,sign_sim_hash):
        no_matching_video={}
        recall_num_of_single_2_group = 0
        no_matching_list = []
        num_of_time_zero = 0
        #从sign_sim_hash中挑选出没有被匹配的单个视频，并将其删除。放入no_maching_video中。
        for sign, videos in sign_sim_hash.items():
            if  len(sign_sim_hash[sign])==1:
                no_matching_video[sign] = sign_sim_hash[sign][0]
                if sign_sim_hash[sign][0]['video_seconds'] == 0:
                    num_of_time_zero += 1
                del sign_sim_hash[sign]
       
        #先让单个的视频和已经匹配的视频进行匹配
        for s1, v1 in no_matching_video.items():
                video1 = v1
                flag = False
                time1 = video1['video_seconds']
                title1 = video1['video_title']
                for s2, vs2 in sign_sim_hash.items():
                    if flag == True:
                        break
                    for v2 in vs2:
                        video2 = v2
                        time2 = video2['video_seconds']
                        title2 = video2['video_title']
                        if abs(time1-time2)<20 and (abs(len(title1)-len(title2))/max(len(title1),len(title2))<0.5):
                          
                            title_sim,seconds_sim,uptime_sim  = self.video_hash_.similar(video1,video2)
                            if (title_sim >= 0.7):                    
                                video1['video_sign']=s2
                                recall_num_of_single_2_group+=1
                                sign_sim_hash[s2].append(video1)
                                del no_matching_video[s1]
                                flag = True
                                break
        for s, v in no_matching_video.items():
            
            no_matching_list.append(v)

        recall_num_of_single_and_single = 0
        del_map = {}
        for idx_1, video_1 in enumerate(no_matching_list):
            time_1  = video_1['video_seconds']
            title_1 = video_1['video_title']
            sign_1  = video_1['video_sign']
            for video_2 in no_matching_list[idx_1+1:]:
                time_2  = video_2['video_seconds']
                title_2 = video_2['video_title']
                sign_2  = video_2['video_sign']

                if abs(time_1-time_2)<20 and (abs(len(title_1)-len(title_2))/max(len(title_1),len(title_2))<0.5):
                    title_sim,seconds_sim,uptime_sim = self.video_hash_.similar(video_1,video_2)

                    if(title_sim>=0.8):

                        flag1 = del_map.get(sign_1)
                        flag2 = del_map.get(sign_2)

                        if not flag1 and not flag2:
                            recall_num_of_single_and_single +=2
                            sign_sim_hash[sign_1] = []
                            sign_sim_hash[sign_1].append(video_1)
                            sign_sim_hash[sign_1].append(video_2)
                            del_map[sign_1] = 1
                            del_map[sign_2] = 1
                        elif flag1:
                            recall_num_of_single_and_single +=1
                            if sign_sim_hash.has_key(sign_1): 
                                sign_sim_hash[sign_1].append(video_2)
                                del_map[sign_2] = 1
                            else:
                                new_key = self.sign(video_1)
                                sign_sim_hash[new_key] = []
                                sign_sim_hash[new_key].append(video_2)
                                del_map[sign_2] =1
                       
                               
        for s,v in no_matching_video.items():
            if not del_map.has_key(s): 
                video_sign = v['video_sign']
                sign_sim_hash[video_sign]=[]
                sign_sim_hash[video_sign].append(v)
        
        self.logger.info('the num of video that recall from single to group is %d, from single to single is %d' % (recall_num_of_single_2_group,recall_num_of_single_and_single))
        self.logger.info('the num of the 0 seconds video is %d'% num_of_time_zero )
        return sign_sim_hash                  
        
    

    def recall(self,sign_sim_hash):

        no_matching_video={}
        restore = []
        
        #从sign_sim_hash中挑选出没有被匹配的单个视频，并将其删除。放入no_maching_video中。
        for sign, videos in sign_sim_hash.items():
            if  len(sign_sim_hash[sign])==1:
                no_matching_video[sign] = sign_sim_hash[sign][0]
                del sign_sim_hash[sign]
        
        #先让单个的视频和已经匹配的视频进行匹配
        for s1, v1 in no_matching_video.items():       
                video1 = v1
                flag = False
                for s2, vs2 in sign_sim_hash.items():
                    if flag:
                        break;
                    for v2 in vs2:
                        video2 = v2
                        title_sim,seconds_sim,uptime_sim  = self.video_hash_.similar(video1,video2)
                        if (title_sim +seconds_sim >= 1.6):
                            video1['video_sign']=s2
                            sign_sim_hash[s2].append(video1)  
                            del no_matching_video[s1]         
                            flag = True
                            break;

        for sign, video in no_maching_video.items():
            video_sign = video['video_sign']
            sign_sim_hash[video_sign]=[]
            sign_sim_hash[sign].append(video)
            
            restore.append(video)


        for idx_1, video_1 in enumerate(restore):
                for video_2 in restore[idx_1+1:]:
                    vid_1, vid_2  = video_1['video_id'], video_2['video_id']
                    sign_1,sign_2 = video_1['video_sign'], video_2['video_sign']
           
                    if sign_1 and sign_2:continue
                    title_sim,seconds_sim,uptime_sim = self.video_hash_.similar(video_1,video_2)

                    if title_sim + seconds_sim > 1.6:

                        if not sign_1 and not sign_2:
                            sign_tmp = self.sign(video_1)
                            video_sign_hash[vid_1] = sign_tmp
                            video_sign_hash[vid_2] = sign_tmp
                            sign_sim_hash[sign_tmp].append(video_1)
                            sign_sim_hash[sign_tmp].append(video_2)

                            
                        elif sign_1:
                            video_sign_hash[vid_2] = sign_1
                            sign_sim_hash[sign_1].append(video_2)
                        elif sign_2:
                            video_sign_hash[vid_1] = sign_2
                            sign_sim_hash[sign_1].append(video_2)
        for video in restore:
            if 'video_sign' not in video or not video['video_sign']:
                video['video_sign'] = self.sign(video)
                sign_tmp=video['video_sign']
                sign_sim_hash[sign_tmp].append(video)
        return sign_sim_hash
                            



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
