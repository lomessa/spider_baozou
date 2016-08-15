#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:Xiaoqi Wang
# 时间：2016-05-31 

from crawler_base import *
import urllib


def parse_video_time(s):
    video_time,unit = 0,1
    for t in s.split(":")[::-1]:
        video_time += unit * int(t)
        unit  = unit * 60
    return video_time


class HandlerBili(HandlerBase):

    def __init__(self, **kargs):
        #
        HandlerBase.__init__(self, site_name="bilibili", db=kargs.get('db',None))
        self.page_total = -1


    def get_page_links(self,video_json,requrl):
        if not video_json:
            self.logger.error("[%s] invalid video json" % requrl)
            return []
        if not video_json.get('status',False):
            self.logger.error("[%s] invalid status" % requrl)
            return []
        ret = []
        if self.page_total < 0:
            self.page_total = video_json['data']['pages']
        for video in video_json.get('data',{}).get('vlist',[]):
            video_publish_time = time.strptime(video['created'], "%Y-%m-%d %H:%M:%S")
            video_publish_time = int(time.mktime(video_publish_time))
            ret.append({
                'url':"http://www.bilibili.com/video/av%s/" % video['aid'],
                'video_id':video.get('aid',''),
                'video_seconds':parse_video_time(video['length']),
                'video_title':video['title'],
                'video_play_num':video['play'],
                'video_publish_time':video_publish_time,
                'crawl':False
            })
        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret


    def get_user_info(self,user_json,requrl):
        if not user_json or not user_json.get('status',False):
            return None
        ret = {'play_num':-1,'follow_num':-1}
        play_num = user_json.get('data',{}).get('playNum',-1)
        follow_num = user_json.get('data',{}).get('fans',-1)
        return {'play_num':play_num,'follow_num':follow_num}


    def process(self, item, requrl):
        item['site_name'] = self.site_name
        item['site_video_id'] = item['video_id']
        item['video_url'] = item['url']
        item['user_id'] = self.user_id
        item['video_upload_time'] = item['video_publish_time']
        video_id = self.db.save_video_info(item)
        if not video_id:return False

        static = {'video_id':video_id}
        static['static_time'] = int(time.time())/3600*3600
        static['play_num'] = item['video_play_num']
        static['other_num'] = ""
        if video_id in self.index_id:
            static['show_in_index'] = 1
        else:
            static['show_in_index'] = 0
        return self.db.save_video_static(static)


    def get_next_link(self,soup,requrl):
        '''
        http://space.bilibili.com/ajax/member/getSubmitVideos?mid=883968&pagesize=30&tid=0&keyword=&page=1
        '''
        url_parse = urlparse.urlparse(requrl)
        query = urlparse.parse_qs(url_parse.query,True)
        query_new = {}
        for key,value in query.items():
            if key == 'page':
                if self.page_total > 0 and int(value[0]) >= self.page_total:
                    return None
                query_new[key] = int(value[0]) + 1
            else:
                query_new[key] = value[0]
        url_new = urlparse.urlunparse([url_parse.scheme,url_parse.netloc,url_parse.path,\
            url_parse.params,urllib.urlencode(query_new),''])
        return url_new


    def index(self):
        ret = set()
        url = "http://www.bilibili.com/index/ding.json"
        index_json = self.get_link_json({'url':url})
        if index_json:
            for cate,obj in index_json.items():
                if not isinstance(obj,dict):continue
                for id_, video in obj.items():
                    if isinstance(video,dict) and 'aid' in video:
                        ret.add(video['aid'])
        self.index_id = ret
        return ret


    def run(self,task):
        index_url = task.get('index_url','')
        url = task.get('url','')
        index_json = HandlerBase.get_link_json(self,{'url':index_url})
        static = self.get_user_info(index_json,index_url)
        if static:
            static['user_id'] = task.get('user_id','')
            static['static_time'] = int(time.time())/3600*3600
            self.db.save_user_static(static)


        self.start_url,self.max_pages = url, task.get("max_pages",50)
        time_interval = task.get('time_interval',1)
        self.user_id = task.get('user_id',-1)

        requrl,cur_page = self.start_url,0
        while True:
            if not requrl:break
            cur_page += 1
            if cur_page > self.max_pages and self.max_pages > 0:break 
            video_json = self.get_link_json({'url':requrl})
            if not video_json:break
            try:
                link_list = self.get_page_links(video_json, requrl)
                if link_list:
                    for item in link_list:
                        self.process(item,requrl)
                else:
                    self.logger.info("url[%s] has no item process", requrl)
                    break
            except Exception as e:
                self.logger.warn("url[%s] process failed, errmsg=%s" % (requrl,str(e)))
            nextlink = self.get_next_link(video_json, requrl)
            if not nextlink:
                self.logger.info("url[%s] has no next page", requrl)
                break
            else:
                requrl = nextlink
            time.sleep(time_interval)
        self.logger.info("start_url[%s], get [%d] pages",self.start_url,cur_page)



# ***************** begin crawl and into database ***********************

def start():
    from libs import uniq_config as uniq_conf
    db = BzHandler()
    crawler_bilibili = HandlerBili(db=db)
    crawler_bilibili.index()
    all_bilibili_users = db.get_users("bilibili")
    if len(all_bilibili_users) % uniq_conf.CRAWLER_NUM == 0:
        every_num = len(all_bilibili_users)/uniq_conf.CRAWLER_NUM
    else:
        every_num = len(all_bilibili_users)/uniq_conf.CRAWLER_NUM + 1
    for user in all_bilibili_users[uniq_conf.CRAWLER_ID*every_num:(uniq_conf.CRAWLER_ID+1)*every_num]:
        user_id, index_url, video_start_url = user['user_id'],user['index_url'],user['video_start_url']
        crawler_bilibili.run({
            'user_id':user_id,'index_url':index_url,'url':video_start_url,'time_interval':1,'max_pages':100
        })



def test():
    db = BzHandler()
    crawler_bilibili = HandlerBili(db=db)
    index_url = "http://space.bilibili.com/ajax/member/GetInfo?mid=883968"
    index_json = crawler_bilibili.get_link_json({'url':index_url})
    print crawler_bilibili.get_user_info(index_json,index_url)
    


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(test())
    start()


