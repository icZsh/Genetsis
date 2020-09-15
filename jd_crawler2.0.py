#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from pynput.keyboard import Key, Controller as c2
# from pynput.mouse import Button, Controller as c1
from selenium.webdriver.remote.webdriver import WebDriver
from time import sleep
import datetime
import pandas as pd
from bs4 import BeautifulSoup

# chrome_options 初始化选项
chrome_options = webdriver.ChromeOptions()
# 设置浏览器初始 位置x,y & 宽高x,y
chrome_options.add_argument(f'--window-position={217},{172}')
chrome_options.add_argument(f'--window-size={1200},{1000}')
# 关闭自动测试状态显示 // 会导致浏览器报：请停用开发者模式
# window.navigator.webdriver还是返回True,当返回undefined时应该才可行。

chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
# 关闭开发者模式
chrome_options.add_experimental_option("useAutomationExtension", False)
prefs = {
    'profile.default_content_settings.popups': 0,
    "download.default_directory": "./",
    "download.prompt_for_download": False,
    "plugins.always_open_pdf_externally": True,
    "safebrowsing.enabled": False
    # "profile.managed_default_content_settings.images": 2,
        }
# 禁止图片加载
# prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)
# 设置中文

chrome_options.add_argument('lang=zh_CN.UTF-8')
# 更换头部
chrome_options.add_argument(
    'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"')
# 隐藏浏览器
# chrome_options.add_argument('--headless') #隐藏浏览器
# 部署项目在linux时，其驱动会要求这个参数
# chrome_options.add_argument('--no-sandbox')
# 创建浏览器对象
script = '''
Object.defineProperty(navigator, 'webdriver', {
    get: () => undefined
})
'''
#Chromedriver指定路径
driver = webdriver.Chrome(executable_path="/Users/zhushenghua/chromedriver",options=chrome_options)
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": script})
driver.maximize_window()

driver.get('https://search.jd.com/Search?keyword=%E6%B8%B8%E6%88%8F%E6%9C%AC&enc=utf-8&wq=%E6%B8%B8%E6%88%8F%E6%9C%AC&pvid=b3a744b76b8d487a93d693ef5954838c')
html=driver.page_source
soup = BeautifulSoup(html,'html.parser', from_encoding='utf-8')
x=soup.find_all(class_='gl-i-wrap')

def get_data_item(item):
    data = {
        '产品名称':item.find('div',attrs={'class':'p-name p-name-type-2'}).em.text,
        '价格':item.find('div',attrs={'class':'p-price'}).i.text,
        '店铺名称':item.find('span',attrs={'class':'J_im_icon'}).a['title'],
        '图片链接':'http:'+item.find('img')['src'],
        '评价数量':item.find('div',class_="p-commit").a.text
    }
    return data

def get_all_items(eles):
    data_list = []
    for i in range(0,30):
        data_list.append(get_data_item(eles[i]))
    return data_list

if __name__ == '__main__':
    result=pd.DataFrame(get_all_items(x))
    print(result)
    result.to_excel('/Users/zhushenghua/Downloads/完整价格信息.xlsx', index=False)