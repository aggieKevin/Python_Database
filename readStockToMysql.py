# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 15:40:09 2018

@author: kevin he
"""

import pandas as pd
import sqlalchemy
import fix_yahoo_finance as yf
import datetime
from my_stock_list import *
engine=sqlalchemy.create_engine('mysql+pymysql://root:hejia123@127.0.0.1:3306/stocks')

start_date=datetime.date(2017,6,1)
for stock in stock_list:    
    try:
        stock_data=yf.download(stock,start=start_date)
        stock_data.to_sql(
                name=stock,
                con=engine,
                index=True,
                if_exists='replace'
                )
        
    except Exception as e:
        print(e)
        print('cannot read stock {0}'.format(stock))        

    
#df=pd.read_sql_table('tsla',engine,index_col='Date')

