#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:Xiaoqi Wang
# 时间：2016-05-31 

from crawler_base import *


def parse_time_str(time_str):
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}",time_str):
            time_arr = time.strptime(time_str, "%Y-%m-%d")
            time_stamp = int(time.mktime(time_arr))
            return time_stamp
        elif time_str[-3:] == "小时前":
            time_stamp =int(time.time()) - int((time_str[:-3])) * 3600
            return time_stamp
        return 0
    except:
        return 0



class HandlerTencent(HandlerBase):

    def __init__(self, **kargs):
        HandlerBase.__init__(self, site_name="tencent", db=kargs.get('db',None))



    def get_video_id(self,url):
        if not url:return False
        video_id_match = re.search(r"http://v\.qq\.com/cover/(.*?)/(.*?).html\?vid=(.*?).*",url)
        if video_id_match:return video_id_match.group(3) 
        video_id_match = re.search(r"http://v\.qq\.com/page/(.*?)/(.*?)/(.*?)/(.*?).html.*",url)
        if video_id_match:return video_id_match.group(4) 
        video_id_match = re.search(r"http://v\.qq\.com/cover/(.*?)/(.*?)/(.*?).html.*",url)
        if video_id_match:return video_id_match.group(3) 
        video_id_match = re.search(r"http://v\.qq\.com/cover/(.*?)/(.*?).html.*",url)
        if video_id_match:return video_id_match.group(2) 
        return False

    def get_play_num(self,video_id,try_times = 3):
        if not video_id:return False
        times = 0
        while times < try_times:
            url = 'http://data.video.qq.com/fcgi-bin/data?tid=376&appid=10001007&appkey=e075742beb866145&callback=jQuery19107640470021851125_1465714408921&idlist='+video_id+'&otype=json&_=1465714408932'
            # reader = urllib2.urlopen("http://data.video.qq.com/fcgi-bin/data?tid=376&appid=10001007&appkey=e075742beb866145&callback=jQuery19107640470021851125_1465714408921&idlist="+video_id+"%2Cp03059gi17g&otype=json&_=1465714408932")
            strHtml = self.get_link_content({'url':url})
            stat_match = re.search(r"jQuery19107640470021851125_1465714408921\((.*)\)",strHtml)
            stat_content = stat_match.group(1) if stat_match else ""
            video_play_num = -1
            if stat_content:
                stat  = json.loads(stat_content)
                video_play_num = stat['results'][0].get('fields').get('view_all_count')
                return video_play_num
            time.sleep(times + 1)
            times += 1

        return False


    def get_page_links(self,soup,requrl):
        strHtml = urllib2.urlopen(requrl).read()
        url_match = re.search(r"jQuery19104429437456715428_1465719431272\((.*)\)",strHtml)
        ret = []
        url_content = url_match.group(1) if url_match else ""
        if url_content:
            data  = json.loads(url_content)
            if data["videolst"]:
                for video_block in data["videolst"]:
                    video_url = video_block["url"]
                    video_title = video_block["title"]
                    video_publish_time_block = video_block["uploadtime"]
                    if video_publish_time_block:
                        video_publish_time = parse_time_str(video_publish_time_block)
                    else:
                        video_publish_time = 0
                    video_id = video_block["vid"]
                    video_play_num_txt = video_block["play_count"]
                    video_play_num_txt = video_play_num_txt.replace(",","")
                    if video_play_num_txt[-1] == '万':
                        video_play_num_txt = video_play_num_txt[:-1]
                        video_play_num = float(video_play_num_txt)*10000
                    else:
                        video_play_num = int(video_play_num_txt)
                     # get accurate play number if upload in past 30 days 
                    if video_publish_time > int(time.time()) - 86400*30:
                        tmp_video_play_num = self.get_play_num(video_id)
                        time.sleep(1)
                        video_play_num = tmp_video_play_num if tmp_video_play_num else video_play_num

                    video_seconds,unit = 0, 1
                    video_time_block = video_block["duration"]
                    for ts in video_time_block.split(":")[::-1]:
                        video_seconds += int(ts)*unit
                        unit *= 60
                    if video_id:
                        ret.append({
                            'url':video_url,
                            'video_id':video_id,
                            'video_seconds':video_seconds,
                            'video_title':video_title,
                            'video_play_num':video_play_num,
                            'video_publish_time':video_publish_time,
                            'crawl':False
                    })
            else:
                return None

        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret



    def get_user_info(self,soup,requrl):

        user_play_num_static,user_follow_num_static = -1,-1
        try:
            user_play_num_block = soup.find("span",{"class":"num"})
            if user_play_num_block:
                user_play_num_txt = user_play_num_block.text
            else:
               user_play_num_txt = "-1"

            if user_play_num_txt[-1] == '万':
                user_play_num_txt = user_play_num_txt[:-1]
                user_play_num_static = float(user_play_num_txt)*10000
            else:
                user_play_num_static = int(user_play_num_txt)       

            user_follow_num_block = soup.find("span",{"class":"num j_rss_count"})
            if user_follow_num_block:
                user_follow_num_txt = user_follow_num_block.text
            else:
               user_follow_num_txt = 0

            if user_follow_num_txt[-1] == '万':
                user_follow_num_txt = user_follow_num_txt[:-1]
                user_follow_num_static = float(user_follow_num_txt)*10000
            else:
                user_follow_num_static = int(user_follow_num_txt) 
        except Exception as e:
            self.logger.error("get user static failed, errmsg=%s" % str(e))

        if user_play_num_static >= 0 and user_follow_num_static >= 0:
            return {'play_num':int(user_play_num_static),'follow_num':int(user_follow_num_static)}
        return None

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
        url_parse = urlparse.urlparse(requrl)
        query = urlparse.parse_qs(url_parse.query,True)
        next = str(int(query["pagenum"][0]) + 1)
        url = "http://c.v.qq.com/vchannelinfo?otype=" + query["otype"][0] + "&uin=" + query["uin"][0] + "&qm=" + query["qm"][0] + "&pagenum=" + next + "&num=" + query["num"][0] + "&sorttype=" + query["sorttype"][0] + "&orderflag=" + query["orderflag"][0] + "&callback=" + query["callback"][0] + "&low_login=" + query["low_login"][0] + "&_=" + query["_"][0]
        return url


    def index(self):
        result = set()
        try:
            soup = HandlerBase.get_link_soup(self,{'url':"http://v.qq.com/"})
            main_block = soup.find("div",{"class":"navigation_inner cf"})
            if main_block:
                for index_page in main_block.find_all("li",{"class":"list_item list_item_hassub"}):
                    time.sleep(1)
                    url = index_page.find('a').get('href')
                    if url != "javascript:;" :index_soup = HandlerBase.get_link_soup(self,{'url':url})
                    for ablock in index_soup.find_all('a'):
                        video_url = ablock.get('href') if ablock else ''
                        if not video_url:continue
                        video_id = self.get_video_id(video_url)
                        if video_id:result.add(video_id)
        except Exception as e:
            self.logger.info("get index id set failed %s" % str(e))
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


# ***************** begin crawl and into database ***********************

def start():

    from libs import uniq_config as uniq_conf

    db = BzHandler()
    crawler_tencent= HandlerTencent(db=db)
    crawler_tencent.index()

    all_tencent_users = db.get_users("tencent")
    if len(all_tencent_users) % uniq_conf.CRAWLER_NUM == 0:
        every_num = len(all_tencent_users)/uniq_conf.CRAWLER_NUM
    else:
        every_num = len(all_tencent_users)/uniq_conf.CRAWLER_NUM + 1

    for user in all_tencent_users[uniq_conf.CRAWLER_ID*every_num:(uniq_conf.CRAWLER_ID+1)*every_num]:
        user_id, index_url, video_start_url = user['user_id'],user['index_url'],user['video_start_url']
        crawler_tencent.run({
            'user_id':user_id,'index_url':index_url,'url':video_start_url,'time_interval':2
        })



def test():
    db = BzHandler()
    crawler_tencent= HandlerTencent(db=db)
    print crawler_tencent.get_play_num('k0305af2gm7')


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(test())
    start()


