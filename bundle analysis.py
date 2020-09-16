#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import os
from gen_selenium import GenSelenium
from bs4 import BeautifulSoup
import re
from tqdm import tqdm_notebook as tqdm
from source import ZWJH
import traceback


def shorten_df(df):
    """
    :param df: original dataframe
    :return: the shortened dataframe with only product names and product links
    """
    new_df = df.rename(columns={'宝贝标题': 'product_name', '宝贝链接': 'product_link'})
    new_df = new_df[['product_name', 'product_link']]
    return new_df


# 将淘数据上下载的数据放在本地的指定文件夹里，并进行读取；我们需要下载每个店铺近三个月来的热销宝贝
def get_whole_product_list(path):
    path_list = os.listdir(path)
    path_list = filter(lambda x: '.xls' in x, path_list)
    result = []
    for filename in path_list:
        excel = pd.read_excel(os.path.join(path, filename), encoding='gb2312')
        result.append(excel)
        df = pd.concat(result)
        df = shorten_df(df)
        df = df.drop_duplicates('product_link').reset_index().drop('index', axis=1)
    return df


# 用来提取soup中的产品特性
def get_data_dict(elements):
    """
    :param elements: bs4 element
    :return: product characteristics
    """
    data = {}
    for ele in elements:
        key = None
        value = None
        blank_text = ele.text
        if ":" in blank_text:
            key = blank_text.split(':')[0].strip()
            value = blank_text.split(':')[1].strip()
        elif "：" in blank_text:
            key = blank_text.split('：')[0].strip()
            value = blank_text.split('：')[1].strip()
        if key is not None and value is not None:
            data[key] = value

    return data


# 给dataframe设置一个名为Bundle的新列，值为True则为套装，值为False则不是套装，判断的标准为产品名称以及网页上的产品参数
def get_bundles_details(df):
    """
    :param df: product list
    :return: possible bundle products
    """
    gen_s = GenSelenium(headless=False)
    volumn_list = []
    Category_list = []
    df['Bundle'] = False
    df['name_bundle'] = df['product_name'].str.contains('套组|套装|组合|\+|礼盒|搭配|件套')
    for url in tqdm(df['product_link']):
        gen_s.driver.get(url)
        html = gen_s.driver.page_source
        soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
        try:
            x = soup.find_all(id='J_AttrUL')
            dict = get_data_dict(x[0].find_all('li'))
            volumn = dict['化妆品净含量']
            volumn_list.append(float(re.findall("\d+\.?\d{0,}", volumn)[0]))
            if '面部精华单品' in dict.keys():
                name = dict['面部精华单品']
            elif '产品名称' in dict.keys():
                name = dict['产品名称']
            elif '品名' in dict.keys():
                name = dict['品名']
            else:
                name = dict['品牌']
            Category_list.append(name)
        except:
            traceback.print_exc()
            continue

    df['Volumn'] = pd.Series((v for v in volumn_list))
    df['Category'] = pd.Series((v for v in Category_list))
    df['category_bundle'] = df['Category'].str.contains('套组|套装|组合|\+|礼盒|搭配|件套').fillna(0)
    for i in range(len(df)):
        if (df['category_bundle'][i] + df['name_bundle'][i]) == 0:
            df['Bundle'][i] = False
        else:
            df['Bundle'][i] = True
    df = df[['product_name', 'product_link', 'Bundle']]
    return df.loc[df['Bundle'] == True]


# 去掉防晒霜产品，因为有可能和部分套装会有检测冲突
def remove_sunscreen(df):
    return df.loc[df['product_name'].str.contains('spf50+|SPF50+|SPF100+') == False]


# 获取套装的价格信息
def get_bundle_price(url):
    """
    :param url: product links
    :return: prices in time series
    """
    x = ZWJH()
    file = []
    for original_url in tqdm(url):
        file.append(x.get_data_form_url(original_url))

    return pd.concat(file)


# 计算产品每毫升的价格
def price_per_volumn(df):
    df['proportion'] = df['price'] / df['volumn']
    return df


# 获取套装中单品的价格以及规格
def get_product_detail(url, volumn):
    x = ZWJH()
    result = x.get_data_form_url(url)
    result['product_link'] = url
    result['volumn'] = volumn
    return result


# 根据单品生成字典
def get_product_dict(products, amt):
    """
    :param products: list of individual products and their volumes
    :param amt: amount of products in the bundle
    :return:
    """
    dff = {}
    for i, item in zip(range(0, amt), products['product_name'].unique()):
        dff[i] = products.loc[products['product_name'] == item]
        dff[i] = price_per_volumn(dff[i])
    return dff


# 将单品的每毫升价格带入套装中的规格，计算出同等毫升下单品和套装的价格区别
def count_sum(volume_array, dff, bundle):
    """
    :param volume_array: volumes of products in the bundle
    :param dff: the product dictionary
    :param bundle: the information of the bundle in the price dataframe
    :return:
    """
    for i in range(len(dff) - 1):
        if len(dff[i]) < len(dff[i + 1]):
            new_df = dff[i]
        else:
            new_df = dff[i + 1]

    new_df['sum'] = 0

    for i in range(len(dff)):
        new_df['sum'] = new_df['sum'] + volume_array[i] * dff[i]['proportion']

    new_df = new_df[['date', 'sum']]
    result = pd.merge(new_df, bundle, how='left', on='date')
    result = result[['date', 'product_name', 'sum', 'price']]
    return result


# 抓取所有套装
path = '/Users/Rita/Price Analysis/bundle/isdin'
df = get_whole_product_list(path)
bundle_list = remove_sunscreen(get_bundles_details(df)).reset_index().drop(['index'], axis=1)

# 获得价格信息price dataframe
bundle_price = get_bundle_price(bundle_list['product_link'])
price_df = pd.merge(bundle_list, bundle_price, on=['product_name'], how='left')
price_df = price_df.dropna(subset=['date', 'price'])

# 这一步需要手动输入单品的url以及容量规格并生成一个字典
pro1 = get_product_detail(
    'https://detail.tmall.com/item.htm?spm=a1z10.1-b-s.w10971653-22720726319.5.67a84586LFgmgr&id=590618303934&rn=ef32ad795ae686188000a44e202b707e&abbucket=19&scene=taobao_shop',
    60)
pro2 = get_product_detail(
    'https://detail.tmall.com/item.htm?spm=a1z10.1-b-s.w10971653-22720726319.1.67a84586LFgmgr&id=586461160577&rn=ef32ad795ae686188000a44e202b707e&abbucket=19&scene=taobao_shop',
    30)
result = pd.concat([pro1, pro2])
dff = get_product_dict(result, 2)

# 这里是一个例子，取的套装为Martiderm美白淡斑净白瓶，可自行修改
bundle=price_df.loc[price_df['product_name']=='MartiDerm玛蒂德肤西班牙安瓶美白淡斑净白瓶 光润焕颜md小安瓶']
bundle_volumn_arr=[20,30]
Martiderm_bundle1=count_sum(bundle_volumn_arr,dff,bundle)
Martiderm_bundle1['brand']='MartiDerm'
print(Martiderm_bundle1)
