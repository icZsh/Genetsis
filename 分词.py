#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import pymysql
import datetime
import numpy as np
import pkuseg
from collections import Counter


def conn():
    """
    :return: 连接到的服务器
    """
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
    """
    :param days: 需要今天之前多少天的数据（天数）
    :return: 那天是几号
    """
    today = datetime.date.today()
    date = today - datetime.timedelta(days=days)
    return date


sql = 'select text,publish_time from wb_wc_article_list'
article_list = pd.read_sql(sql, conn())
# 获取90天内的数据
start_date = get_start_date(90)
# 对dataframe进行条件筛选以获取90天内的数据
list_ = article_list[article_list['publish_time'] > np.datetime64(start_date)]

# Initialize Pkuseg,详见 https://github.com/lancopku/pkuseg-python
seg = pkuseg.pkuseg()
# stopword-zh.txt在bitbucket上，需要下载到本地的当前路径
stopwords = pd.read_csv('./stopwords-zh.txt', encoding='utf8', names=['stop_word'],
                        index_col=False)
stop_list = stopwords['stop_word'].tolist()
df = list_.copy()
# 对数据进行分词分析；去掉stopword中的词，对剩下的词进行分词
df.loc[:, 'word'] = df.loc[:, 'text'].apply(lambda x: [i for i in seg.cut(x) if i not in stop_list])
new_df = df[['text', 'word']]

words = []
for content in new_df['word']:
    words.extend(content)

# 词频统计
result = Counter(words).most_common(10)
print(result)
