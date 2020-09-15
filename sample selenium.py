#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from selenium import webdriver

driver.get('http://www.weather.com.cn/html/province/jiangsu.shtml')
ele = driver.find_element_by_id("forecastID")
print(ele.text)

# -----------------------------------
# 再从 forecastID 元素获取所有子元素dl
html_doc = ele.get_attribute('innerHTML')

from bs4 import BeautifulSoup
soup = BeautifulSoup(html_doc, "html5lib")

# 发现每个城市的信息都在dl里面
dls = soup.find_all('dl')

# 将城市和气温信息保存到列表citys中
citys = []
for dl in dls:
    name = dl.a.string
ltemp = dl.b.string
ltemp = int(ltemp.replace('℃',''))
    print(name, ltemp)
    citys.append([name,ltemp])

lowest = None
lowestCitys = []  # 温度最低城市列表
for one in citys:
    curcity = one[0]
    ltemp = one[1]
    # 发现气温更低的城市
    if lowest==None or ltemp<lowest:
        lowest = ltemp
        lowestCitys = [curcity]
    #  温度和当前最低相同，加入列表
    elif ltemp ==lowest:
        lowestCitys.append(curcity)