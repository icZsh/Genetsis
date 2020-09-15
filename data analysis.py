#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
import pymysql
import numpy as np
import matplotlib.pyplot as plt

pg_username = 'deploy'
pg_password = 'Genetsis2019@'
pg_host = 'rm-uf66010lgas9h78138o.mysql.rds.aliyuncs.com'
pg_port = 3306
pg_database = 'historical_prices'

pymysql_conn = pymysql.connect(pg_host, pg_username, pg_password, pg_database)
sql = 'select * from Price_sale_table'
df= pd.read_sql(sql,pymysql_conn)
print(df)

