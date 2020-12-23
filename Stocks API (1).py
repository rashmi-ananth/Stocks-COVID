#!/usr/bin/env python
# coding: utf-8

# In[2]:


import urllib3
from urllib3 import request
# to handle certificate verification
import certifi
# to manage json data
import json
# for pandas dataframes
import pandas as pd


# In[3]:


# handle certificate verification and SSL warnings
# https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
http = urllib3.PoolManager(
       cert_reqs='CERT_REQUIRED',
       ca_certs=certifi.where())


# In[41]:


import requests
import json 
key = 'XXX'
ticker = 'SWPPX'

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&apikey={}'.format(ticker, key)
r = http.request('GET', url)
r.status
data = json.loads(r.data.decode('utf-8'))
data
df = pd.DataFrame(data)
df.head(30)
df2 = df.iloc[5:]


df2 = df2.rename(columns={'Meta Data': 'MD', 'Time Series (Daily)': 'TSD'})

df2['Open'] = '0'   
df2['High'] = '0'
df2['Low'] = '0'
df2['Close'] = '0'


for i in range(len(df2)):
    df2['Open'][i] = df2['TSD'][i]['1. open']
    df2['High'][i] = df2['TSD'][i]['2. high']
    df2['Low'][i] = df2['TSD'][i]['3. low']
    df2['Close'][i] = df2['TSD'][i]['4. close']


df2['Open'] = pd.to_numeric(df2['Open'])
df2['High'] = pd.to_numeric(df2['High'])
df2['Low'] = pd.to_numeric(df2['Low'])
df2['Close'] = pd.to_numeric(df2['Close'])
df2["Average"] = (df2['High'] + df2['Low'])/2

df3 = df2[::-1]
df3.plot( y='Average')
df3 = df2[::-1]
df2.head(10)



# In[ ]:




