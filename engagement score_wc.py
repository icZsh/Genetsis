#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import math

class Engagement_wc(object):

    def __init__(self, article_dynamic: pd.DataFrame, article_list: pd.DataFrame):
        """
        initialize the article_dynamic dataframe and article_list dataframe for Engegement_wc class
        :param article_dynamic:
        :param article_list:
        """
        self.article_dynamic = article_dynamic
        self.article_list = article_list

    def merge_and_format(self) -> pd.DataFrame:
        """
        merge the dynamic and article_list dataframes and deal with data formatting
        :return: the merged dataframe
        """
        df = pd.merge(self.article_dynamic, self.article_list, on=['__biz', 'sn'], how='left')
        df['comment_count'] = df['comment_count'].fillna(round(df['read_num'] * 0.02))
        df = df[['sn', 'publish_time', 'read_num', 'like_num', 'comment_count', '__biz', 'title', 'cover']]
        df['publish_time'] = pd.to_datetime(df['publish_time'], infer_datetime_format=True).dt.normalize()
        return df

    def estimate_followers(self) -> pd.DataFrame:
        """
        estimate the number of followers for each official WeChat accounts by reads
        :return: the new dataframe with followers' counts
        """
        self.today = datetime.date.today()
        self.start_date = self.today - datetime.timedelta(days=180)
        new_df = self.merge_and_format()
        result = new_df[new_df['publish_time'] > np.datetime64(self.start_date)]
        dict_ = (result.groupby('__biz').mean()['read_num'] * 100).to_dict()
        print(dict_)
        new_df['followers_count'] = [dict_[store] for store in new_df['__biz']]
        return new_df

    def modify_value(self, x) -> float:
        """
        modify the raw engagement score
        :param x: the value
        :return: the modified value
        """
        if x >= 10:
            return math.log(x)
        else:
            return x

    def remove_outofbound(self, x) -> float:
        """
        set the value to 10 if the value is greater than 10
        :param x: the raw engagement score
        :return: the new engagement score
        """
        if x >= 10:
            return 10
        else:
            return x

    def get_engagement_score(self) -> pd.DataFrame:
        """
        calculate the engagement scores
        :return: the final dataframe with engagement scores
        """
        df = self.estimate_followers()
        df['raw_score'] = df['read_num'] * 0.1 + df['like_num'] * 0.4 + df['comment_count'] * 0.5
        df['engagement_score'] = df['raw_score'] / df['followers_count'] * 1000
        df['engagement_score'] = df['engagement_score'].apply(lambda x: self.modify_value(x))
        df['engagement_score'] = df['engagement_score'].apply(lambda x: self.remove_outofbound(x))
        df.drop('raw_score', axis=1, inplace=True)
        return df


if __name__ == '__main__':
    df1=pd.read_csv('/Users/zhushenghua/test1.csv')
    df2=pd.read_csv('/Users/zhushenghua/test2.csv')
    test = Engagement_wc(df1,df2)
    print(test.get_engagement_score())
