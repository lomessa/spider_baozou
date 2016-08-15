#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2016-05-31 

from crawler_base import *

import urllib2

def parse_time_str(time_str):
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}",time_str) or re.match(r"\d{4}-\d{1}-\d{2}",time_str) or re.match(r"\d{4}-\d{2}-\d{1}",time_str) or re.match(r"\d{4}-\d{1}-\d{1}",time_str):
            time_arr = time.strptime(time_str, "%Y-%m-%d")
            time_stamp = int(time.mktime(time_arr))
            return time_stamp      
        elif time_str == "昨天":
            time_stamp =int(time.time()) - 24 * 3600
            return time_stamp
        elif time_str[-3:] == "小时前":
            time_stamp =int(time.time()) - int((time_str[:-3])) * 3600
            return time_stamp 
        elif time_str == "刚刚":
            time_stamp = int(time.time())   
            return time_stamp
        return 0
    except:
        return 0



class HandlerIqiyi(HandlerBase):

    def __init__(self, **kargs):
        HandlerBase.__init__(self, site_name="iqiyi", db=kargs.get('db',None))
        self.video_test = dict()
        self.index_id = set()


    def get_play_num(self,tvid):
        video_play_num = -1
        url = "http://mixer.video.iqiyi.com/jp/mixin/videos/%s?callback=window.Q.__callbacks__.cbmvxcrb&status=1" % tvid
        content = self.get_link_content({'url':url})
        stat_match = re.search(r"try\{window\.Q\.__callbacks__\.cbmvxcrb\((.*?)\);\}catch\(e\)\{\};.*",content)
        stat_content = stat_match.group(1) if stat_match else ""
        if stat_content:
            stat  = json.loads(stat_content)
            video_play_num = stat["data"]["playCount"]
      	return video_play_num


	
    def get_page_links(self,soup,requrl):
        ret = []

        for video_block in soup.find_all('li',{"j-delegate":'colitem'}):
            ablock = video_block.find("div",{"class":"site-piclist_pic"}).find('a')
            if not ablock:continue
            video_url = ablock.get("href")
            video_title = ablock.get("data-title")
            video_id_match = re.search(r"http://www\.iqiyi\.com/[a-z]_(.*?)\.html.*",video_url)
            video_id = video_id_match.group(1) if video_id_match else ""
            
            if video_id not in self.video_test:
                self.video_test[video_id] = {'num':0,"videos":[]}
            self.video_test[video_id]['num'] += 1
            self.video_test[video_id]['videos'].append({'url':video_url,'title':video_title})

            publish_time_block = video_block.find("span",{"class":"playTimes_status tl"})
            if publish_time_block:
                video_publish_time_block = publish_time_block.text[:-2]
                video_publish_time = parse_time_str(video_publish_time_block)         
            else:
                video_publish_time = 0

            tv_id = video_block.get("tvid").replace(",","")
            # video_play_num = -1
            # if video_publish_time > int(time.time()) - 86400*30:
            video_play_num = self.get_play_num(tv_id)
            # elif int(time.time())%86400/3600 == 1:
            #    video_play_num = self.get_play_num(tv_id)
            
            video_seconds,unit = 0, 1
            video_time_txt =  video_block.find("span",{"class":"mod-listTitle_right"}).text
            for ts in video_time_txt.split(":")[::-1]:
                video_seconds += int(ts)*unit
                unit *= 60
            ret.append({
                'url':video_url,
                'video_id':video_id,
                'video_seconds':video_seconds,
                'video_title':video_title,
                'video_play_num':video_play_num,
                'video_publish_time':video_publish_time,
                'crawl':False
            })

        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret


    def get_link_soup(self,task):
        content = self.get_link_content(task)
        if not content:
            self.fetcher = Fetcher(self.logger)
            content = self.get_link_content(task)
        if not content:
            return None
        try:
            m = re.search(r"try\{window\.Q\.__callbacks__\.cbc5pamo\((.*)\)\}catch\(e\)\{\}",content)
            if not m:return None
            content = m.group(1)
            content = json.loads(content)
            soup = BeautifulSoup(content['data'],'html.parser')
            return soup
        except Exception as e:
            self.logger.warn("url[%s] get_link_soup failed, errmsg=%s",task.get('url',''),str(e))
        return None


    def get_user_info(self,soup,requrl):
        user_play_num_static,user_follow_num_static = -1, -1
        try:
            if not soup:return None
            user_play_num_block = soup.find("span",{"class":"conn_type S_line1"})
            if user_play_num_block:
                user_play_num_txt = user_play_num_block.find('a').text
            else:
                user_play_num_txt = -1

            if user_play_num_txt and user_play_num_txt[-1] == '万':
                user_play_num_static = float(user_play_num_txt[:-1])*10000
            elif user_play_num_txt and user_play_num_txt[-1] == '亿':
                user_play_num_static = float(user_play_num_txt[:-1])*100000000
            elif user_play_num_txt:
                user_play_num_static = int(user_play_num_txt)
        
            user_follow_num_block = soup.find("span",{"class":"conn_type"})
            if user_follow_num_block:
                user_follow_num_static = int(user_follow_num_block.find('a').get('data-countnum'))
        except Exception as e:
             self.logger.warn("unable to find user static data, errmsg = %s" % str(e))
        
        if user_play_num_static < 0 or user_follow_num_static < 0:
            return None
        user = {'play_num':int(user_play_num_static),'follow_num':int(user_follow_num_static)}
        return user

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
        if static['play_num'] >= 0:
            return self.db.save_video_static(static)


    def get_next_link(self,soup,requrl):
        url_parse = urlparse.urlparse(requrl)
        query = urlparse.parse_qs(url_parse.query,True)
        next = str(int(query["page"][0]) + 1)
        url = "http://www.iqiyi.com/u/api/V/video/get_paged_video?page_size=" + query["page_size"][0] + "&uid=" + query["uid"][0] + "&page=" + next + "&sort=" + query["sort"][0] + "&callback=" + query["callback"][0]
        return url


    def index(self):
        result = set()
        try:
            soup = HandlerBase.get_link_soup(self,{'url':"http://www.iqiyi.com/"})
            
            main_block = soup.find("div",{"class":"navPop_bd clearfix"})
            if main_block:
                for index_page in main_block.find_all("li"):
                    time.sleep(1)
                    url = index_page.find('a').get('href')
                    index_soup = HandlerBase.get_link_soup(self,{'url':url})
                    for ablock in index_soup.find_all('a'):
                        video_url = ablock.get('href') if ablock else ''
                        if not video_url:continue
                        video_id_match = re.search(r"http://www\.iqiyi\.com/v_(.*?)\.html.*",video_url)
                        video_id = video_id_match.group(1) if video_id_match else ""
                        if video_id:result.add(video_id)
        except Exception as e:
            self.logger.warn('get iqiyi id set failed, errmsg=%s' % str(e))
        self.index_id = result


    def run(self,task):
        index_url = task.get('index_url','')
        url = task.get('url','')

        index_soup = HandlerBase.get_link_soup(self,{'url':index_url})
        static = self.get_user_info(index_soup,index_url)
        if static:
            static['user_id'] = task.get('user_id','')
            static['static_time'] = int(time.time())/3600*3600
            self.db.save_user_static(static)
        HandlerBase.run(self,task)

        #self.logger.info("video set len:%d, list len:%d" %(len(self.video_id_set_),len(self.video_id_list_)))

# ***************** begin crawl and into database ***********************

def start():
    from libs import uniq_config as uniq_conf
    db = BzHandler()
    crawler_iqiyi= HandlerIqiyi(db=db)
    crawler_iqiyi.index()
    all_iqiyi_users = db.get_users("iqiyi")
    if len(all_iqiyi_users) % uniq_conf.CRAWLER_NUM == 0:
        every_num = len(all_iqiyi_users)/uniq_conf.CRAWLER_NUM
    else:
        every_num = len(all_iqiyi_users)/uniq_conf.CRAWLER_NUM + 1
        
    for user in all_iqiyi_users[uniq_conf.CRAWLER_ID*every_num:(uniq_conf.CRAWLER_ID+1)*every_num]:
        user_id, index_url, video_start_url = user['user_id'],user['index_url'],user['video_start_url']
        crawler_iqiyi.run({
            'user_id':user_id,'index_url':index_url,'url':video_start_url,'time_interval':2
        })



def test():
    # db = BzHandler()
    # crawler_iqiyi= HandlerIqiyi(db=db)
    # crawler_iqiyi.index()
    # crawler_iqiyi.run({
    #         'user_id':'1000033','index_url':'http://www.iqiyi.com/u/1061614233','url':'http://www.iqiyi.com/u/api/V/video/get_paged_video?page_size=42&uid=1061614233&page=1&sort=1&callback=window.Q.__callbacks__.cbc5pamo','time_interval':2
    #     })
    pass


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(test())
    start()


