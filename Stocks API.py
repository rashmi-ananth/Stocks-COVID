#!/usr/bin/env python
# coding: utf-8

# In[9]:


import urllib3
from urllib3 import request
# to handle certificate verification
import certifi
# to manage json data
import json
# for pandas dataframes
import pandas as pd


# In[10]:


# handle certificate verification and SSL warnings
# https://urllib3.readthedocs.io/en/latest/user-guide.html#ssl
http = urllib3.PoolManager(
       cert_reqs='CERT_REQUIRED',
       ca_certs=certifi.where())


# In[50]:


import requests
import json 
key = 'XXX'
ticker = 'AMZN'

url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&apikey={}'.format(ticker, key)
r = http.request('GET', url)
r.status
data = json.loads(r.data.decode('utf-8'))
data
df = pd.DataFrame(data)
df.head(30)


# In[14]:




