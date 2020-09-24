#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import pymysql
import datetime
import numpy as np
import pkuseg
from collections import Counter


def conn():
    connect = pymysql.connect(
        host='rm-uf66010lgas9h78138o.mysql.rds.aliyuncs.com',
        port=3306,
        user='deploy',
        password='Genetsis2019@',
        db='media',
        charset='utf8'
    )
    if connect:
        print('服务器连接成功')
    else:
        print('服务器连接失败')
    return connect


def get_start_date(days):
    today = datetime.date.today()
    date = today - datetime.timedelta(days=days)
    return date


sql = 'select text,publish_time from wb_wc_article_list'
article_list = pd.read_sql(sql, conn())
start_date = get_start_date(90)
list_ = article_list[article_list['publish_time'] > np.datetime64(start_date)]

seg = pkuseg.pkuseg()
stopwords = pd.read_csv('./stopwords-zh.txt', encoding='utf8', names=['stop_word'],
                        index_col=False)
stop_list = stopwords['stop_word'].tolist()
df = list_.copy()
df.loc[:, 'word'] = df.loc[:, 'text'].apply(lambda x: [i for i in seg.cut(x) if i not in stop_list])
new_df = df[['text', 'word']]

words = []
for content in new_df['word']:
    words.extend(content)

result = Counter(words).most_common(10)
print(result)
