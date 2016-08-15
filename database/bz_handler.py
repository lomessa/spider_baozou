#! /usr/bin/env python
# -*- coding:utf-8 -*-
# author:wenhui
# 时间：2016-05-31 
# 描述：暴走抓取数据--增加、查询

from db_handler import *
sys.path.append(os.path.join(os.path.dirname(__file__),'../'))

from libs import config as conf

import logging
import logging.handlers
import logging.config

log_file = os.path.join(os.path.dirname(__file__), '../logs/db.log')
handler = logging.handlers.RotatingFileHandler(log_file,
                maxBytes = 500*1024*1024, backupCount = 3)
fmt = ("%(name)s %(levelname)s %(filename)s:%(lineno)s %(asctime)s"
        " %(process)d:%(thread)d  %(message)s")
handler.setFormatter(logging.Formatter(fmt))
logger = logging.getLogger("db")
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# ************************** Bz Handler Defination *************************

class BzHandler(object):

    def __init__(self):
        self.logger = logger
        self.db = DBHandler(conf.MYSQL_HOST,conf.MYSQL_PORT,conf.MYSQL_DB,
                            conf.MYSQL_USER,conf.MYSQL_PASSWORD,logger)
        self.logger.info("bz database connected ...")


    def get_users(self,site_name,status = 1):
        sql_format = ("select user_id,index_url,video_start_url from bz_user"
                      " where status=%d and site_name='%s' order by user_id")
        sql = sql_format % (status, site_name)
        cursor,count = self.db.execute(sql)
        ret = []
        for user in cursor.fetchall():
            ret.append({
                'user_id':user[0],'index_url':user[1],
                'video_start_url':user[2],
            })
        return ret


    def save_video_info(self,video_info):
        site_name, site_video_id = video_info.get('site_name',''),video_info.get('site_video_id','')
        if not site_name or not site_video_id:
            return False

        save_sql_format = """
            insert into bz_video (`site_name`,`site_video_id`,`video_title`,`video_seconds`,
            `video_upload_time`,`video_url`,`user_id`,`create_time`,`update_time`)
            values (%s,%s,%s,%s,from_unixtime(%s),%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE 
            video_title=values(video_title),video_url=values(video_url),
            user_id=values(user_id),update_time=CURRENT_TIMESTAMP
            """
        self.db.execute_ex(save_sql_format,[
            site_name,site_video_id,video_info['video_title'],video_info['video_seconds'],
            video_info['video_publish_time'],video_info['video_url'],video_info['user_id']
            ])

        select_sql_format = "select video_id from bz_video where site_name=%s and site_video_id=%s"
        cursor, count = self.db.execute_ex(select_sql_format,[site_name, site_video_id])
        for x in cursor.fetchall():
            return x[0]
        return False


    def save_video_static(self,video_static):
        video_id = video_static.get('video_id',0)
        if not video_id:return False

        save_sql_format = """
            insert into bz_video_static_hour (`video_id`,`static_time`,`play_num`,`other_num`,
            `show_in_index`,`create_time`,`update_time`)
            values (%s,from_unixtime(%s),%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE 
            play_num=values(play_num),other_num=values(other_num), 
            show_in_index=values(show_in_index),update_time=CURRENT_TIMESTAMP
            """
        
        cursor, count = self.db.execute_ex(save_sql_format,[video_static['video_id'],
                        video_static['static_time'],video_static['play_num'],
                        video_static['other_num'],video_static['show_in_index']])
        return count == 1


    def save_user_static(self,user_static):
        user_id = user_static.get("user_id",0)
        if not user_id: return False

        save_sql_format = """insert into bz_user_static_hour (`user_id`,`static_time`,`play_num`,`follow_num`,
            `create_time`,`update_time`) values(%s,from_unixtime(%s),%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON DUPLICATE KEY UPDATE play_num=values(play_num),follow_num=values(follow_num),update_time=CURRENT_TIMESTAMP"""
        cursor, count = self.db.execute_ex(save_sql_format,[user_static['user_id'],
                        user_static['static_time'],user_static['play_num'],
                        user_static['follow_num']])
        return count == 1



    def save_video_sign(self,video_id, sign):
        sql_format = "update bz_video set video_sign='%s' where video_id=%d"
        cursor,count = self.db.execute(sql_format % (sign, video_id))
        return count == 1


    def get_video_info(self, last_time):
        sql_format = ("select video_id, site_name, site_video_id, video_title, video_seconds,"
            "video_upload_time,video_url,video_sign,user_id from bz_video where update_time>=from_unixtime(%d)")
        cursor, count = self.db.execute(sql_format % last_time)
        ret = []
        for video in cursor.fetchall():
            ret.append({
                'video_id':video[0],
                'site_name':video[1],
                'site_video_id':video[2],
                'video_title':video[3],
                'video_seconds':video[4],
                'video_upload_time':video[5],
                'video_url':video[6],
                'video_sign':video[7],
                'user_id':video[8]
            })
        return ret


    def get_all_video_info(self, bz_user_id,max_video_id):
        sql = "SELECT user_id FROM bz_user WHERE bz_user_id='%s'" % bz_user_id
        cursor, count = self.db.execute(sql)
        user_id_list = []
        for item in cursor.fetchall():
            user_id_list.append(item[0])

        if not user_id_list:
            return []
        sql_format = """SELECT video_id, site_name, site_video_id, video_title, video_seconds,
            video_upload_time,video_url,video_sign,user_id FROM bz_video WHERE user_id IN (%s)
             AND video_id > %d"""
        cursor, count = self.db.execute(sql_format % (",".join([str(x) for x in user_id_list]),max_video_id))
        ret = []
        for video in cursor.fetchall():
            ret.append({
                'video_id':video[0],
                'site_name':video[1],
                'site_video_id':video[2],
                'video_title':video[3],
                'video_seconds':video[4],
                'video_upload_time':video[5],
                'video_url':video[6],
                'video_sign':video[7],
                'user_id':video[8]
            })
        return ret


    def get_user_info(self,):
        sql = "select user_id,bz_user_id,site_name,site_user_id,index_url,status from bz_user"
        ret = []
        cursor,count = self.db.execute(sql)
        for user in cursor.fetchall():
            ret.append({
                'user_id':user[0],
                'bz_user_id':user[1],
                'site_name':user[2],
                'site_user_id':user[3],
                'index_url':user[4],
                'status':user[5]
            })
        return ret



    def get_video_static(self, static_time):
        sql_format = ("select video_id, static_time, play_num, other_num, show_in_index "
            " from bz_video_static_hour where static_time >= from_unixtime(%d)")
        cursor, count = self.db.execute(sql_format % static_time)
        ret = []
        for static in cursor.fetchall():
            ret.append({
                'video_id':static[0],
                'static_time':static[1],
                'play_num':static[2],
                'other_num':static[3],
                'show_in_index':static[4]
            })
        return ret


    def get_user_static(self, static_time):
        sql_format = ("select user_id, static_time, play_num, follow_num"
            " from bz_user_static_hour where static_time >= from_unixtime(%d)")
        cursor, count = self.db.execute(sql_format % static_time)
        ret = []
        for static in cursor.fetchall():
            ret.append({
                'user_id':static[0],
                'static_time':static[1],
                'play_num':static[2],
                'follow_num':static[3]
            })
        return ret



class BzRemoteHandler(object):

    def __init__(self):
        self.logger = logger
        self.db = DBHandler(conf.REMOTE_MYSQL_HOST,conf.REMOTE_MYSQL_PORT,conf.REMOTE_MYSQL_DB,
                            conf.REMOTE_MYSQL_USER,conf.REMOTE_MYSQL_PASSWORD,logger)
        self.logger.info("bz remote database connected ...")



    def save_video_info(self,video_info):
        site_name, site_video_id = video_info.get('site_name',''),video_info.get('site_video_id','')
        if not site_name or not site_video_id:return False

        save_sql_format = """
            insert into bz_video (`video_id`,`site_name`,`site_video_id`,`video_title`,`video_seconds`,
            `video_upload_time`,`video_url`,`user_id`,`video_sign`,`update_time`)
            values (%s,%s,%s,%s,%s,from_unixtime(%s),%s,%s,%s,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE 
            video_title=values(video_title),video_url=values(video_url),
            user_id=values(user_id),video_sign=values(video_sign),
            update_time=CURRENT_TIMESTAMP
            """
        cursor,count = self.db.execute_ex(save_sql_format,[
            video_info['video_id'],site_name,site_video_id,video_info['video_title'],
            video_info['video_seconds'],video_info['video_upload_time'],video_info['video_url'],
            video_info['user_id'],video_info['video_sign']
            ])
        return count == 1


    def save_user_info(self,user_info):
        save_sql_format = """
            insert into bz_user (`user_id`,`bz_user_id`,`site_name`,`site_user_id`,
            `index_url`,`status`,`update_time`)
            values (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE 
            index_url=values(index_url),status=values(status),
            update_time=CURRENT_TIMESTAMP
            """
        cursor,count = self.db.execute_ex(save_sql_format,[
            user_info['user_id'],user_info['bz_user_id'],
            user_info['site_name'],user_info['site_user_id'],
            user_info['index_url'],user_info['status']
            ])
        return count == 1


    def save_video_static(self,video_static):
        video_id = video_static.get('video_id',0)
        if not video_id:return False

        save_sql_format = """
            insert into bz_video_static_hour (`video_id`,`static_time`,`play_num`,`other_num`,
            `show_in_index`,`update_time`)
            values (%s,%s,%s,%s,%s,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE 
            play_num=values(play_num),other_num=values(other_num), 
            show_in_index=values(show_in_index),update_time=CURRENT_TIMESTAMP
            """
        
        cursor, count = self.db.execute_ex(save_sql_format,[video_static['video_id'],
                        video_static['static_time'],video_static['play_num'],
                        video_static['other_num'],video_static['show_in_index']])
        return count == 1


    def save_user_static(self,user_static):
        user_id = user_static.get("user_id",0)
        if not user_id: return False

        save_sql_format = """insert into bz_user_static_hour (`user_id`,`static_time`,`play_num`,`follow_num`,
                `update_time`) values(%s,%s,%s,%s,CURRENT_TIMESTAMP) 
            ON DUPLICATE KEY UPDATE play_num=values(play_num),follow_num=values(follow_num),update_time=CURRENT_TIMESTAMP"""
        cursor, count = self.db.execute_ex(save_sql_format,[user_static['user_id'],
                        user_static['static_time'],user_static['play_num'],
                        user_static['follow_num']])
        return count == 1


    def save_video_sign(self,video_id, sign):
        sql_format = "update bz_video set video_sign='%s' where video_id=%d"
        cursor,count = self.db.execute(sql_format % (sign, video_id))
        return count == 1




if __name__ == "__main__":
    db = BzHandler()
    print db.get_users("youku")
