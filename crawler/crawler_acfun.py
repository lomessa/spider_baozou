#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:Xiaoqi Wang
# 时间：2016-05-31 

from crawler_base import *
import urllib


class HandlerAcfun(HandlerBase):

    def __init__(self, **kargs):
        #
        HandlerBase.__init__(self, site_name="acfun", db=kargs.get('db',None))
        self.page_total = -1


    def get_page_links(self,video_json,requrl):
        if not video_json:
            self.logger.error("[%s] invalid video json" % requrl)
            return []
        ret = []

        if self.page_total < 0:
            self.page_total = video_json['page']['totalPage']
        for video in video_json.get('contents',[]):
            ret.append({
                'url':urlparse.urljoin("http://www.acfun.tv/",video['url']),
                'video_id':video.get('aid',video.get('cid','')),
                'video_seconds':video['time'],
                'video_title':video['title'],
                'video_play_num':video['views'],
                'video_publish_time':video['releaseDate']/1000,
                'crawl':False
            })
        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret


    def get_user_info(self,soup,requrl):
        ret = {'play_num':-1,'follow_num':-1}
        if not soup:return None
        info_block = soup.find("div",{'class':'area-extra'})
        if not info_block:return None
        for ablock in info_block.find_all("a"):
            atext = ablock.text
            if not atext:continue
            atext = atext.strip()
            follow_match = re.search(ur"(\d+)听众",atext)
            if follow_match:
                ret['follow_num'] = int(follow_match.group(1))
                break
        return ret


    def process(self, item, requrl,**kargs):
        user_static = kargs.get('user_static',None)
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
        if user_static:
            user_static['play_num'] += item['video_play_num']

        static['other_num'] = ""
        if video_id in self.index_id:
            static['show_in_index'] = 1
        else:
            static['show_in_index'] = 0
        return self.db.save_video_static(static)


    def get_next_link(self,soup,requrl):
        '''
        http://www.acfun.tv/u/contributeList.aspx?userId=268722&pageSize=20&pageNo=1&channelId=0
        '''
        url_parse = urlparse.urlparse(requrl)
        query = urlparse.parse_qs(url_parse.query,True)
        query_new = {}
        for key,value in query.items():
            if key == 'pageNo':
                if self.page_total > 0 and int(value[0]) >= self.page_total:
                    return None
                query_new[key] = int(value[0]) + 1
            else:
                query_new[key] = value[0]
        url_new = urlparse.urlunparse([url_parse.scheme,url_parse.netloc,url_parse.path,\
            url_parse.params,urllib.urlencode(query_new),''])
        return url_new


    def index(self):
        def extract_vid(soup):
            ret = set()
            if not soup:ret
            for ablock in soup.find_all("a"):
                url = ablock.get('href')
                if not url or not url.strip():continue
                vid_match = re.search(r'http://www.acfun.tv/v/ac(\d+)";',url.strip())
                if not vid_match:continue
                ret.add(vid_match.group(1))
            return ret

        result = set()
        try:
            soup = HandlerBase.get_link_soup(self,{'url':"http://www.acfun.tv/"})
            result = result | extract_vid(soup)
            main_block = soup.find("nav",id="nav")
            if main_block:
                for block in main_block.find_all("li")[:10]:
                    ablock = block.find('a')
                    if not ablock:continue
                    url = ablock.get('href')
                    if not url or not url.startswith("http://www.acfun.tv"):continue
                    index_soup = HandlerBase.get_link_soup(self,{'url':url})
                    result = result | extract_vid(index_soup)
                    time.sleep(1)
        except Exception as e:
            self.logger.info("get index id set failed %s" % str(e))
        self.index_id = result


    def run(self,task):
        index_url = task.get('index_url','')
        url = task.get('url','')
        index_soup = HandlerBase.get_link_soup(self,{'url':index_url})
        user_static = self.get_user_info(index_soup,index_url)

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
                        self.process(item,requrl,user_static=user_static)
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

        if user_static:
            user_static['user_id'] = task.get('user_id','')
            user_static['static_time'] = int(time.time())/3600*3600
            self.db.save_user_static(user_static)

        self.logger.info("start_url[%s], get [%d] pages",self.start_url,cur_page)



# ***************** begin crawl and into database ***********************

def start():
    from libs import uniq_config as uniq_conf
    db = BzHandler()
    crawler_acfun= HandlerAcfun(db=db)
    crawler_acfun.index()
    all_acfun_users = db.get_users("acfun")
    if len(all_acfun_users) % uniq_conf.CRAWLER_NUM == 0:
        every_num = len(all_acfun_users)/uniq_conf.CRAWLER_NUM
    else:
        every_num = len(all_acfun_users)/uniq_conf.CRAWLER_NUM + 1
    for user in all_acfun_users[uniq_conf.CRAWLER_ID*every_num:(uniq_conf.CRAWLER_ID+1)*every_num]:
        user_id, index_url, video_start_url = user['user_id'],user['index_url'],user['video_start_url']
        crawler_acfun.run({
            'user_id':user_id,'index_url':index_url,'url':video_start_url,'time_interval':1,'max_pages':100
        })



def test():
    db = BzHandler()
    crawler_acfun= HandlerAcfun(db=db)

    index_url = 'http://www.acfun.tv/u/268722.aspx#area=post-history'
    index_soup = crawler_acfun.get_link_soup({'url':index_url})
    print crawler_acfun.get_user_info(index_soup,index_url)
    


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(test())
    start()


