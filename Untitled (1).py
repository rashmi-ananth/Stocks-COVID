#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


# In[7]:


train = pd.read_csv('train.csv')


# In[8]:


train


# In[5]:


sns.barplot(x='Sex', y='Pclass', data=train)


# In[15]:


train[train['Age'] <= 15]['PassengerId'].count()


# In[18]:


train['Age'].max()


# In[19]:


train[train['Age'] <= 12]['PassengerId'].count()


# In[20]:


train[train['Age'] >= 13 & (train['Age'] <= 18)]['PassengerId'].count()


# In[30]:


X_train_data = pd.DataFrame({'Age':[0,13,19,30,60,80]})

bins= [0,13,19,30,60,80]
labels = ['Kid','Teen','Adult','Mid life crisis','Oldie']
X_train_data['AgeGroup'] = pd.cut(X_train_data['Age'], bins=bins, labels=labels, right=False)
X_train_data['Survived'] = -1
print (X_train_data)


# In[26]:


X_train_data['AgeGroup']


# In[32]:


train


# In[ ]:




