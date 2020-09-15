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
import pandas as pd

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

#打开浏览器
driver.get("https://www.jd.com/")
searchword = driver.find_element_by_xpath('//*[@id="key"]')
searchword.send_keys("游戏本")

JD_search = driver.find_element_by_xpath('//*[@id="search"]/div/div[2]/button')
JD_search.click()
sleep(10)
#获取商品名称以及价格
eles = driver.find_elements_by_xpath('//*[@id="J_goodsList"]/ul/li[*]/div/div[*]/a/em')
prices = driver.find_elements_by_xpath('//*[@id="J_goodsList"]/ul/li[*]/div/div[*]/strong/i')
product_list=[]
price_list=[]
for ele in eles:
    product_list.append(ele.text)
product_list=pd.DataFrame(product_list,columns=['Names'])
for price in prices:
    price_list.append(price.text)
price_list=pd.DataFrame(price_list,columns=['Prices'])

#保存结果
result=pd.concat([product_list,price_list],axis=1)
print(result)

#导出为excel文件
result.to_excel('/Users/zhushenghua/Downloads/价格信息.xlsx',index=False)