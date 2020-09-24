#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd


class ROI(object):
    def __init__(self, ecom, comm):
        self.ecom = ecom
        self.comm = comm

    def ecom_clean_up(self):
        df = self.ecom
        df = df.drop(['Path'], axis=1).rename(columns={'Title': 'Campaign Name',
                                                     'Approval?': 'Approved?',
                                                     'Type of Activity': 'Campaign Type',
                                                     'Campaign Name': 'Client',
                                                     'CCP': 'Channel',
                                                     'CCP Details': 'Channel Details'})
        df['Site'] = 'ECommerce List'
        return df

    def comm_clean_up(self):
        df = self.comm
        df = df.drop(['Path'], axis=1).rename(columns={'Start Date': 'Campaign Start Date',
                                                     'End Date': 'Campaign End Date',
                                                     'Warm Up Start Date': 'Warm up Start Date',
                                                     'Warm Up End Date': 'Warm up End Date',
                                                     'In/Out Platform?': 'In/Out Platform'})
        df['Site'] = 'Promo List'
        df['SKU'] = df['SKU'].apply(str)
        return df

    def concat(self):
        return pd.concat([self.ecom_clean_up(), self.comm_clean_up()])


if __name__ == '__main__':
    ecom = pd.read_excel('/Users/zhushenghua/Downloads/Ecom.xlsx')
    comm = pd.read_excel('/Users/zhushenghua/Downloads/Communication.xlsx')
    result = ROI(ecom, comm).concat()
    result.to_excel('/Users/zhushenghua/Downloads/test.xlsx')
    print(result)