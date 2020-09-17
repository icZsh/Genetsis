#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import pymysql
from gen_rds_utils import DBUtils


class Database(object):
    def __init__(self):
        self.conn = pymysql.connect(
            host='rm-uf66010lgas9h78138o.mysql.rds.aliyuncs.com',
            port=3306,
            user='deploy',
            password='Genetsis2019@',
            charset='utf8'
        )

    def get_wb_article(self):
        sql1 = "SELECT * FROM media.ods_weibo_article"
        weibo = pd.read_sql(sql1, self.conn)
        return weibo

    def get_wc_article(self):
        sql2 = "SELECT * FROM wechat.wechat_article_list"
        wechat_article_list = pd.read_sql(sql2, self.conn)
        return wechat_article_list

    def get_wc_article_dynmaic(self):
        sql3 = "SELECT * FROM wechat.wechat_article_dynamic"
        wechat_article_dynamic = pd.read_sql(sql3, self.conn)
        return wechat_article_dynamic

    def close_database(self):
        self.conn.close()


brand_list = {'MzA4MDE5MjkxNA==': 'Clinique',
              'MzI1NjQyMjgzMQ==': 'Martiderm',
              'MzI4NzM3MDkzMA==': 'Isdin',
              'MzAwMTgzNTY0Nw==': 'Filorga',
              'MjM5NzU4ODU3NA==': 'Caudalie',
              'MzU1NTk0MTY2OA==': 'Sesderma',
              'MzI0MDEwNjk3Mg==': 'Bella Aurora',
              'MzkxODAyMzI0MA==': 'Martiderm',
              'MzUzMjcwMTY2MA==': 'Avene',
              'MzUyNTQ1NjY1Ng==': 'Darphin',
              'MzI2MjMyNTE1MA==': 'Dermina'}


class Concat(object):

    def __init__(self, wc_article, wc_dynamic, weibo):
        self.wc_article = wc_article
        self.wc_dynamic = wc_dynamic
        self.weibo = weibo

    def clean_wc(self):
        df_wc = pd.merge(self.wc_dynamic, self.wc_article, on=['__biz', 'sn'], how='left')
        wechat_df = df_wc.copy()
        wechat_df = wechat_df[
            ['sn', 'publish_time', 'read_num', 'like_num', 'comment_count', '__biz', 'title', 'cover']]
        wechat_df.loc[:, 'publish_time'] = pd.to_datetime(wechat_df.loc[:, 'publish_time'],
                                                          infer_datetime_format=True).dt.normalize()
        wechat_df.loc[:, 'platform'] = 'WeChat'
        wechat_df['__biz'] = wechat_df['__biz'].replace(brand_list)
        wechat_df = wechat_df.rename(columns={'__biz': 'account',
                                              'comment_count': 'comment',
                                              'like_num': 'read_like',
                                              'title': 'text'})
        return wechat_df

    def clean_wb(self):
        df_wb = self.weibo[
            ['name', 'article_id', 'text', 'pics', 'created_date', 'attitudes_count', 'comments_count',
             'reposts_count']]
        weibo_df = df_wb.copy()
        weibo_df.loc[:, 'platform'] = 'Weibo'
        weibo_df.loc[:, 'created_date'] = pd.to_datetime(weibo_df.loc[:, 'created_date'],
                                                         infer_datetime_format=True).dt.normalize()
        weibo_df = weibo_df.rename(columns={'created_date': 'publish_time',
                                            'name': 'account',
                                            'article_id': 'sn',
                                            'attitudes_count': 'read_like',
                                            'comments_count': 'comment',
                                            'reposts_count': 'repost',
                                            'pics': 'cover'})
        return weibo_df

    def concat_df(self):
        df = pd.concat([self.clean_wc(), self.clean_wb()], ignore_index=True)
        df = df[
            ['platform', 'account', 'publish_time', 'sn', 'read_num', 'read_like', 'comment', 'repost', 'text',
             'cover']]
        return df


class PushDatabase(object):
    def __init__(self, df):
        self.config = {
            "host": "rm-uf66010lgas9h78138o.mysql.rds.aliyuncs.com",
            "port": 3306,
            "user": 'deploy',
            "password": 'Genetsis2019@',
            "charset": 'utf8mb4'
        }
        self.df = df

    def push_to_db(self):
        dbutils = DBUtils(self.config)
        db = "media"
        dbutils.push_table_df(df=self.df, db=db, table="wb_wc_article_list_test", if_exists="append")


if __name__ == '__main__':
    wechat_article_list = Database().get_wc_article()
    wechat_article_dynamic = Database().get_wc_article_dynmaic()
    weibo = Database().get_wb_article()
    Database().close_database()
    test = Concat(wechat_article_list, wechat_article_dynamic, weibo)
    concat_result = test.concat_df()
    PushDatabase(concat_result).push_to_db()
