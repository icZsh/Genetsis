#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import math
import numpy as np


class EngagementScore(object):
    """
    the EngagementScore
    """

    def __init__(self, article_df: pd.DataFrame, account_df: pd.DataFrame):
        """
        init the EngagementScore class and prepare the data
        :param article_df: account_df
        :param account_df: account_df
        :return:
        """
        self.article_df = article_df
        self.account_df = account_df

    def modify_value(self, x) -> float:
        """
        modify the raw engagement score
        :param x: the value
        :return: the modified value
        """
        if x >= 5:
            return math.log(x)
        else:
            return math.sqrt(x)

    def remove_outofbound(self, x) -> float:
        """
        set the value to 10 if the value is greater than 10
        :param x:raw engagement score
        :return:new engagement score
        """
        if x >= 10:
            return 10
        else:
            return x

    def merge_df(self) -> pd.DataFrame:
        """
        merge article_df and account_df
        :return:the result
        """
        self.account_df['user_id'] = self.account_df['user_id'].astype('int')
        self.df = pd.merge(self.article_df, self.account_df,
                           on=['user_id',
                               'name',
                               'brand',
                               'screen_name',
                               'date'],
                           how='left')
        self.df = self.df[['created_date', 'name', 'brand', 'user_id', 'screen_name', 'article_id',
                           'text', 'video_url', 'location', 'pics', 'source', 'attitudes_count', 'comments_count',
                           'reposts_count', 'topics', 'at_users',
                           'followers_count']]
        self.df['followers_count'] = self.df['followers_count'].astype('float')
        return self.df

    def get_engagement_score(self) -> pd.DataFrame:
        """
        calculate the engagement score
        :return: the result of the engagement score data frame
        """
        df = self.merge_df()
        df['raw_score'] = df['attitudes_count'] * 0.2 + df['comments_count'] * 0.3 + df['reposts_count'] * 0.5
        df['engagement_score'] = np.divide(df['raw_score'], df['followers_count']) * 10000
        df['engagement_score'] = df['engagement_score'].apply(lambda x: self.modify_value(x))
        df['engagement_score'] = df['engagement_score'].apply(lambda x: self.remove_outofbound(x))
        df.drop(['raw_score', 'followers_count'], axis=1, inplace=True)
        return df


if __name__ == '__main__':
    df1 = pd.read_csv('/Users/zhushenghua/test3.csv')
    df2 = pd.read_csv('/Users/zhushenghua/test4.csv')
    e = EngagementScore(df1, df2)
    res = e.get_engagement_score()
    print(res)
