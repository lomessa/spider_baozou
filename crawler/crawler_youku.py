#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2016-05-31 

from crawler_base import *
import urllib

def parse_time_str(time_str):
    if isinstance(time_str,unicode):
        time_str = time_str.encode("utf-8")
    try:
        cur_time = int(time.time())
        if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",time_str):
            time_arr = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            time_stamp = int(time.mktime(time_arr))
            return time_stamp
        elif re.match(r"\d{4}-\d{2}-\d{2}",time_str):
            time_arr = time.strptime(time_str, "%Y-%m-%d")
            time_stamp = int(time.mktime(time_arr))
            return time_stamp
        elif re.match(r"(\d+)小时前",time_str):
            ret =  cur_time - int(re.match(r"(\d+)小时前",time_str).group(1))*3600
            return ret
        elif re.match(r"([^\d]*?) (\d{2}):(\d{2})",time_str):
            m = re.match(r"(.*?) (\d{2}):(\d{2})",time_str)
            dv, hour,minute = m.group(1), int(m.group(2)), int(m.group(3))
            before_days = 0
            if dv == '昨天':
                before_days = 1
            elif dv == '前天':
                before_days = 2
            ret = cur_time/86400*86400 - before_days*86400 + hour*3600 + minute * 60
            return ret
        elif re.match(r"(\d+)天前", time_str):
            ret = cur_time - int(re.match(r"(\d+)天前", time_str).group(1))*86400
            return ret
        elif re.match(r"\d{2}-\d{2} \d{2}:\d{2}",time_str):
            time_str = time.strftime("%Y", time.localtime(cur_time)) + "-" + time_str + ":00"
            time_arr = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            time_stamp = int(time.mktime(time_arr))
            return time_stamp
        return 0
    except Exception as e:
        print traceback.format_exc()
        return 0


class HandlerYouku(HandlerBase):

    def __init__(self, **kargs):
        pass
        HandlerBase.__init__(self, site_name="youku", db=kargs.get('db',None))


    def get_play_num(self,video_id,try_times = 3):
        if not video_id:return False
        times = 0
        while times < try_times:
            url = "http://v.youku.com/QVideo/~ajax/getVideoPlayInfo"
            url += "?__rt=1&__ro=&id=%s&sid=0&type=vv" % video_id
            ret_dict = self.get_link_json({'url':url})
            if ret_dict and 'vv' in ret_dict:
                return ret_dict['vv']
            time.sleep(times + 1)
            times += 1
        return False


    def get_page_links_i(self,soup,requrl):
        ret = []
        video_blocks = soup.find("div",{"class":"items"})
        if not video_blocks:
            self.logger.warn("url[%s] has no video list" % requrl)
            return []
        for video_block in video_blocks.find_all("div",{"class":"v va"}):
            ablock = video_block.find("div",{"class":"v-link"}).find("a")
            if not ablock:continue
            video_url = ablock.get("href")
            video_title = ablock.get("title")

            video_publish_time_block = video_block.find("span",{"class":"v-publishtime"})
            if video_publish_time_block:
                video_publish_time = parse_time_str(video_publish_time_block.text)
            else:
                video_publish_time = 0

            video_id_match = re.search(r"http://v\.youku\.com/v_show/id_(.*?).html.*",video_url)
            video_id = video_id_match.group(1) if video_id_match else ""
            video_play_num_block = video_block.find("span",{"class":"v-num"})
            video_play_num_txt  = video_play_num_block.text if video_play_num_block else ""
            video_play_num_txt = video_play_num_txt.replace(",","")

            video_play_num = 0
            if video_play_num_txt and video_play_num_txt[-1] == '万':
                video_play_num = float(video_play_num_txt[:-1])*10000
            elif video_play_num_txt:
                video_play_num = int(video_play_num_txt)

            # get accurate play number if upload in past 30 days 
            if video_publish_time > int(time.time()) - 86400*30:
                tmp_video_play_num = self.get_play_num(video_id)
                time.sleep(1)
                video_play_num = tmp_video_play_num if tmp_video_play_num else video_play_num

            video_seconds,unit = 0, 1
            video_time_block = video_block.find("span",{"class":"v-time"})
            video_time_txt = video_time_block.text if video_time_block else ""
            for ts in video_time_txt.split(":")[::-1]:
                video_seconds += int(ts)*unit if ts else 0
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

        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret


    def get_page_links_u(self,soup,requrl):
        ret = []
        for video_block in soup.find_all("div",{"class":"yk-col4"}):     
            video_publish_time_txt = video_block.get("c_time")
            video_publish_time = parse_time_str(video_publish_time_txt) if video_publish_time_txt else ""
            ablock = video_block.find("div",{"class":"v-link"})
            if not ablock or not ablock.find("a"):continue
            ablock = ablock.find("a")
            video_title = ablock.get("title")
            video_url = ablock.get("href")
            video_id_match = re.search(r"http://v\.youku\.com/v_show/id_(.*?).html.*",video_url)
            video_id = video_id_match.group(1) if video_id_match else ""
            video_play_num_block = video_block.find("span",{"class":"v-num"})
            video_play_num_txt  = video_play_num_block.text if video_play_num_block else ""
            video_play_num_txt = video_play_num_txt.replace(",","")
            video_play_num = int(video_play_num_txt) if video_play_num_txt else -1
            video_seconds,unit = 0, 1
            video_time_block = video_block.find("span",{"class":"v-time"})
            video_time_txt = video_time_block.text if video_time_block else ""
            for ts in video_time_txt.split(":")[::-1]:
                video_seconds += int(ts)*unit
                unit *= 60

            ret.append({
                'url':video_url,
                'video_id':video_id,
                'video_title':video_title,
                'video_seconds':video_seconds,
                'video_play_num':video_play_num,
                'video_publish_time':video_publish_time,
                'crawl':False
            })

        self.logger.info("url[%s] extract %d page links" % (requrl,len(ret)))
        return ret


    def get_link_soup(self,task):
        requrl = task['url']
        try:
            count = 0
            while count < 5:
                content = self.get_link_content(task)
                if not content:
                    count += 1
                    continue
                soup = BeautifulSoup(content,"html.parser")
                page_links = self.get_page_links(soup,requrl)
                if page_links:
                    return soup               
                self.fetcher = Fetcher(self.logger)
                count += 1
        except Exception as e:
            self.logger.warn("url[%s] get_link_soup failed, errmsg=%s",task.get('url',''),str(e))
        return None


    def get_user_info_i(self,soup,requrl):
        user_play_num_block = soup.find("li",{"class":"vnum"})
        if user_play_num_block:
            user_play_num_static = user_play_num_block.get("title")
            user_play_num_static = user_play_num_static.replace(",","")
        else:
           user_play_num_static = 0

        user_follow_num_block = soup.find("li",{"class":"snum"})
        if user_follow_num_block:
            user_follow_num_static  = user_follow_num_block.get("title")
            user_follow_num_static  = user_follow_num_static.replace(",","")
        else:
           user_follow_num_static = 0
        user = {'play_num':int(user_play_num_static),'follow_num':int(user_follow_num_static)}
        return user


    def get_user_info_u(self,soup,requrl):
        user_play_num_block = soup.find("li",{"class":"vnum"})
        if user_play_num_block:
            user_play_num_txt = user_play_num_block.find("em").text.replace(",","")
        else:
           user_play_num_txt = 0
        if user_play_num_txt and user_play_num_txt[-1] == '万':
            user_play_num_static = float(user_play_num_txt[:-1])*10000
        elif user_play_num_txt and user_play_num_txt[-1] == '亿':
            user_play_num_static = float(user_play_num_txt[:-1])*100000000
        elif user_play_num_txt:
            user_play_num_static = int(user_play_num_txt)

        user_follow_num_block = soup.find("li",{"class":"snum"})
        if user_follow_num_block:
            user_follow_num_txt  = user_follow_num_block.find("em").text
        else:
           user_follow_num_txt = 0
        user_follow_num_txt = user_follow_num_txt.replace(",","")
        if user_follow_num_txt and user_follow_num_txt[-1] == '万':
            user_follow_num_static = float(user_follow_num_txt[:-1])*10000
        elif user_follow_num_txt and user_follow_num_txt[-1] == '亿':
            user_follow_num_static = float(user_follow_num_txt[:-1])*100000000
        elif user_follow_num_txt:
            user_follow_num_static = int(user_follow_num_txt)
        user = {'play_num':int(user_play_num_static),'follow_num':int(user_follow_num_static)}
        return user


    def get_page_links(self,soup,requrl):
        if requrl.startswith("http://i.youku.com/u"):
            return self.get_page_links_u(soup,requrl)
        elif requrl.startswith("http://i.youku.com/i"):
            return self.get_page_links_i(soup,requrl)
        else:
            return []

    def get_user_info(self,soup,requrl):
        if requrl.startswith("http://i.youku.com/u"):
            return self.get_user_info_u(soup,requrl)
        elif requrl.startswith("http://i.youku.com/i"):
            return self.get_user_info_i(soup,requrl)
        else:
            return []


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
        

    def get_next_link_i(self,soup,requrl):
        next = soup.find("li",{"class":"next"})
        ablock = next.find("a") if next else ""
        if ablock:
            url_text =  ablock.get("href") 
            url = "http://i.youku.com" + url_text
        else:
            return None
        return url

    def get_next_link_u(self,soup,requrl):
        url_parse = urlparse.urlparse(requrl)
        query = urlparse.parse_qs(url_parse.query,True)
        query_new = {}
        for key,value in query.items():
            if key == 'page_num':
                query_new[key] = int(value[0]) + 1
            else:
                query_new[key] = value[0]
        url_new = urlparse.urlunparse([url_parse.scheme,url_parse.netloc,url_parse.path,
            url_parse.params,urllib.urlencode(query_new),''])
        return url_new

    def get_next_link(self,soup,requrl):
        if requrl.startswith("http://i.youku.com/u"):
            return self.get_next_link_u(soup,requrl)
        elif requrl.startswith("http://i.youku.com/i"):
            return self.get_next_link_i(soup,requrl)
        else:
            return []

    def index(self):
        result = set()
        try:
            soup = HandlerBase.get_link_soup(self,{'url':"http://www.youku.com/"})
            main_block = soup.find("ul",{"class":"top-nav-main"})
            if main_block:
                for index_page in main_block.find_all("li"):
                    url = index_page.find('a').get('href')
                    index_soup = HandlerBase.get_link_soup(self,{'url':url})
                    if not index_soup: return result
                    for ablock in index_soup.find_all('a'):
                        video_url = ablock.get('href') if ablock else ''
                        if not video_url:continue
                        video_id_match = re.search(r"http://v\.youku\.com/v_show/id_(.*?).html.*",video_url)
                        video_id = video_id_match.group(1) if video_id_match else ""
                        if video_id:result.add(video_id)
        except Exception as e:
            self.logger.warn("get youku index id set failed, errmsg=%s" % str(e))
        self.index_id = result


    def run(self,task):
        
        index_url = task.get('index_url','')
        url = task.get('url','')
        index_soup = HandlerBase.get_link_soup(self,{'url':index_url})
        static = self.get_user_info(index_soup,index_url)
        static['user_id'] = task.get('user_id','')
        static['static_time'] = int(time.time())/3600*3600
        self.db.save_user_static(static)
        HandlerBase.run(self,task)



# ***************** begin crawl and into database ***********************

def start():

    from libs import uniq_config as uniq_conf

    db = BzHandler()
    crawler_youku = HandlerYouku(db=db)
    crawler_youku.index()

    all_youku_users = db.get_users("youku")
    if len(all_youku_users) % uniq_conf.CRAWLER_NUM == 0:
        every_num = len(all_youku_users)/uniq_conf.CRAWLER_NUM
    else:
        every_num = len(all_youku_users)/uniq_conf.CRAWLER_NUM + 1

    for user in all_youku_users[uniq_conf.CRAWLER_ID*every_num:(uniq_conf.CRAWLER_ID+1)*every_num]:
        user_id, index_url, video_start_url = user['user_id'],user['index_url'],user['video_start_url']
        crawler_youku.run({
            'user_id':user_id,'index_url':index_url,'url':video_start_url,'time_interval':2
        })



def test():
    # db = BzHandler()
    # crawler_youku = HandlerYouku(db=db)
    # # crawler_youku.index()
    # crawler_youku.run({
    #         'user_id':'1000010','index_url':'http://i.youku.com/u/UMzAxMTI0OTg4','url':'http://i.youku.com/u/UMzAxMTI0OTg4/videos','time_interval':2
    #     })
    pass



if __name__ == "__main__":
    # inst = HandlerYouku()
    # # test url starts with http://i.youku.com/u/
    # inst.run({'url':'http://i.youku.com/i/UNTY5MDQ5Njcy/videos'})
    # # test url starts with http://i.youku.com/i/
    # #inst.run({'url':'http://i.youku.com/i/UNTMyNDk2Nzgw/videos?order=1&page=7'})
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        sys.exit(test())
    start()
