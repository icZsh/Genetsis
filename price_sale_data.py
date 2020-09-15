#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from gen_selenium import GenSelenium
from time import sleep
from bs4 import BeautifulSoup
import re
import json
import time, datetime
import traceback
from tqdm import tqdm
import uuid
from source import ZWJH
from gen_rds_utils import DBUtils
import collections


def get_source_code(shop, prod_link):
    """
    :param shop: name of the shop
    :param prod_link: link of the product
    :return: the sorted dataframe with sales and revenue
    """
    shop_id = shop_dict.get(shop)
    product_id = re.compile('(?<=id=)\d+\.?\d*').findall(prod_link)[0]
    url = 'https://taosj.com/data/shop/offer/list?api_name=shop_get_offer_list_old&startDate=' + start_timestamp + '&endDate=' + end_timestamp + '&itemId=' + product_id + '&id=' + shop_id
    #     print(url)
    g.driver.get(url)
    html = g.driver.page_source
    soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    js = json.loads(soup.body.text)
    df = pd.DataFrame(js['data'])[['date', 'amount', 'price']].rename(columns={'amount': 'sales', 'price': 'revenue'})
    df['date'] = pd.to_datetime(df['date'], unit='ms', origin=pd.Timestamp('1970-01-01 08:00:00'))
    df.insert(0, 'product_link', prod_link)
    df.insert(0, 'brand', shop)
    return df


def judge(df):
    """
    :param df: the price_sale dataframe
    :return: the dataframe with the additional column: 'Price Label'
    """
    global level_judge
    if pd.isnull(df['price']):
        return 'No Price'
    elif df['price'] <= level_judge['25%'][df['Category']]:
        return 'Low'
    elif df['price'] <= level_judge['75%'][df['Category']]:
        return 'Medium'
    else:
        return 'High'


def product_name_get(df):
    """
    :param df: the price_sale dataframe
    :return: the product name dataframe
    """
    b = collections.Counter(df['Products(CN)'].to_list())
    df = pd.DataFrame([dict(b)]).T.reset_index().rename(columns={"index": 'product_name'})[['product_name']]
    return df


def count_drop(data_result):
    """
    :param data_result: the price_sale dataframe
    :return: price drops of products
    """
    product_name_table = product_name_get(data_result)
    df = {}
    for i, name in zip(range(1, len(product_name_table['product_name'])), product_name_table['product_name']):
        price_data = data_result[data_result['Products(CN)'] == name][['date', 'price', 'Products(CN)', "Brand"]]
        price_data = price_data.sort_values(by=['date'])
        seq = price_data['price'].tolist()
        percentage_list = []
        for next_index in range(1, len(price_data)):
            start_index = next_index - 1
            if seq[start_index] > seq[next_index]:
                percentage = (seq[start_index] - seq[next_index]) / seq[start_index]
                percentage_list.append(percentage)
        b = collections.Counter(percentage_list)
        df[i] = pd.DataFrame([dict(b)]).T.reset_index().rename(columns={0: "count", "index": 'percentage'})
        df[i]['product_name'] = name
    result = pd.concat(df)
    result = result.reset_index(drop=True)
    return result


def get_weighted_ave(df):
    """
    :param df: the price_sale dataframe
    :return: average drop of products
    """
    result = count_drop(df)
    result['multi'] = result['percentage'] * result['count']
    res = result.groupby('product_name').sum()
    res['ave'] = res['multi'] / res['count']
    ave_result = res.drop(['percentage', 'multi'], axis=1)
    ave_result = ave_result.reset_index()
    return ave_result


shop_dict = {'Martiderm': '590164443',
             'Dermina': '108464804',
             'Clinique': '103892217',
             'Sesderma': '332174635',
             'Isdin': '120381598',
             'Filorga': '136469313',
             'Avene': '337635667',
             'Anessa': '127947591',
             'Caudalie': '145670852',
             'Darphin': '588339666',
             'Bella Aurora': '238750113'}

# Log in to TSJ
g = GenSelenium(headless=True)
g.driver.get('https://login.taosj.com/?redirectURL=https%3A%2F%2Fwww.taosj.com%2F')
loginCode = g.driver.find_element_by_name('loginCode')
loginCode.send_keys('13818848270')
sleep(1)
loginPassword = g.driver.find_element_by_name('loginPassword')
loginPassword.send_keys('D8asolution')
sleep(1)
T_Login = g.driver.find_element_by_id('T_Login')
T_Login.click()

# Get timestamps for starting and end dates
today = datetime.date.today()
start_date = today - datetime.timedelta(days=180)
end_date = today - datetime.timedelta(days=1)
start_timestamp = str(int(time.mktime(start_date.timetuple()))) + '000'
end_timestamp = str(int(time.mktime(end_date.timetuple()))) + '000'

# Read the excel file and data cleaning
df = pd.read_excel('/Users/zhushenghua/Downloads/G-commerce/BEL01 2020903 Corresponding products.xlsx',
                   sheet_name=1).fillna(method='ffill')
df['uuid'] = df['Product Link'].apply(lambda x: uuid.uuid3(uuid.NAMESPACE_DNS, x))
df = df[~df['Products(CN)'].isin(['银钻精华多效修护赋活浓缩精华液', '双V安瓶精华维AC修护精华液', '【2支装】防晒霜'])].reset_index().drop('index', axis=1)
ba_df = pd.read_excel('/Users/zhushenghua/Downloads/G-commerce/BEL01 2020903 Corresponding products.xlsx').fillna(
    method='ffill')
ba_df = ba_df.drop(9).reset_index().drop('index', axis=1)
ba_df['uuid'] = ba_df['Product Link'].apply(lambda x: uuid.uuid3(uuid.NAMESPACE_DNS, x))
new_df = pd.concat([df, ba_df], ignore_index=True)
urls = new_df['Product Link'].unique().tolist()

# Get the sales and revenue of products
result = pd.DataFrame(columns=['brand', 'product_link', 'date', 'sales', 'revenue'])
for i in tqdm(range(len(new_df) - 1)):
    temp_df = new_df.iloc[i]
    try:
        info = get_source_code(temp_df['Brand'], temp_df['Product Link'])
        result = pd.concat([result, info], ignore_index=True)
    except:
        traceback.print_exc()
        continue
result['uuid'] = result['product_link'].apply(lambda x: uuid.uuid3(uuid.NAMESPACE_DNS, x))
sale_info = pd.merge(new_df, result, on='uuid', how='left').drop(['product_link', 'uuid', 'brand'], axis=1)

# Get prices of products
file = pd.DataFrame(columns=['date', 'price', 'note', 'product_name'])
x = ZWJH()
for original_url in tqdm(urls):
    price = x.get_data_form_url(original_url)
    price['Product Link'] = original_url
    file = pd.concat([file, price], ignore_index=True, axis=0)
file['uuid'] = file['Product Link'].apply(lambda x: uuid.uuid3(uuid.NAMESPACE_DNS, x))
price_info = pd.merge(new_df, file, on='Product Link', how='left').drop(['uuid_x', 'uuid_y'], axis=1)
price_info['date'] = pd.to_datetime(price_info['date'])

# Merge price and sale tables to one dataframe
price_sale = sale_info.merge(price_info, how='outer')
price_sale['date'] = price_sale['date'].dt.strftime('%Y-%m-%d')
price_sale.loc[price_sale['revenue'].isna(), 'revenue'] = 0

# Add price label to the price_sale dataframe and create price drop table
info_df = price_sale.groupby('Category')['price'].describe()
level_judge = info_df.to_dict()
price_sale['Price Label'] = price_sale.apply(judge, axis=1)
price_drop = get_weighted_ave(price_sale)

# Push the two tables to the database
config = {
    "host": "rm-uf66010lgas9h78138o.mysql.rds.aliyuncs.com",
    "port": 3306,
    "user": 'deploy',
    "password": 'Genetsis2019@',
    "charset": 'utf8mb4'
}
db_utils = DBUtils(config)
db = "bella_aurora_db"
db_utils.push_table_df(df=price_sale, db=db, table="ba_price_sale", if_exists="replace")
db_utils.push_table_df(df=price_drop, db=db, table='ba_price_drop', if_exists="replace")
