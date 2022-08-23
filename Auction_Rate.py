#!/usr/bin/env python
# coding: utf-8

# In[60]:


import pandas as pd
from datetime import date
from datetime import timedelta
import numpy as np
pd.set_option("display.max_rows", None)


# In[61]:


#today = date.today()
#today = today - timedelta(days = 1)
d = pd.read_csv(f".\Scraped_AuctionRate\d_AR_{today}.csv")
R = pd.read_csv(f".\Scraped_AuctionRate\R_AR_{today}.csv")


# # Processing R data

# In[62]:


sub_merg = list(d['suburb'].unique())
R_short = R[R['Sub'].isin(sub_merg)].reset_index(drop = True)
columns = ['Address','Property Type','Status','Price']
for column in columns:
    R_short[f'{column}'] = R_short[f'{column}'].str.lower()
R_short.drop('Zipcode',axis = 'columns')
R_short['City'] =R_short['State'].apply(lambda x: 'sydney' if x == 'nsw' else 'canberra' if x =='act' else 'adelaide' if x =='sa' else 'brisbane' if x=='qld' else 'melbourne')
R_short['Address'] = R_short['Address'].apply(lambda x: x.split(' ')[0:2])
R_short['Address'] = R_short['Address'].apply(lambda x: x[0] +' '+ x[1])


# In[63]:


for column in columns:
    R_short[f'{column}'] = R_short[f'{column}'].str.lower()
R_short = R_short[['Sub','State','City','Address','Property Type','Beds','Status','Price','Zipcode']]


# In[64]:


R_short


# In[65]:


d


# # d Processing

# In[66]:


# d['address'] = d['address'].apply(lambda x: x.replace(' av',' avenue'))
# d['address'] = d['address'].apply(lambda x: x.replace(' bvd',' boulevard'))
# d['address'] = d['address'].apply(lambda x: x.replace(' cr',' crescent'))
# d['address'] = d['address'].apply(lambda x: x.replace(' cct',' circuit'))
# d['address'] = d['address'].apply(lambda x: x.replace(' cir',' circle'))
# d['address'] = d['address'].apply(lambda x: x.replace(' cl',' close'))
# d['address'] = d['address'].apply(lambda x: x.replace(' ct',' court'))
# d['address'] = d['address'].apply(lambda x: x.replace(' dr',' drive'))
# d['address'] = d['address'].apply(lambda x: x.replace(' e',' east'))
# d['address'] = d['address'].apply(lambda x: x.replace(' gdns',' gardens'))
# d['address'] = d['address'].apply(lambda x: x.replace(' gr',' grove'))
# d['address'] = d['address'].apply(lambda x: x.replace(' hwy',' highway'))
# d['address'] = d['address'].apply(lambda x: x.replace(' la',' lane'))
# d['address'] = d['address'].apply(lambda x: x.replace(' lp',' loop'))
# d['address'] = d['address'].apply(lambda x: x.replace(' pde',' parade'))
# d['address'] = d['address'].apply(lambda x: x.replace(' pl',' place'))
# d['address'] = d['address'].apply(lambda x: x.replace(' rd',' road'))
# d['address'] = d['address'].apply(lambda x: x.replace(' ri',' rise'))
# d['address'] = d['address'].apply(lambda x: x.replace(' wy',' way'))
# d['address'] = d['address'].apply(lambda x: x.replace(' wk',' walk'))
# d['address'] = d['address'].apply(lambda x: x.replace(' tce',' terrace'))
# d['address'] = d['address'].apply(lambda x: x.replace(' sq',' square'))
# d['address'] = d['address'].apply(lambda x: x.replace(' st',' street'))
# d['address'] = d['address'].apply(lambda x: x.replace(' steet',' street'))
# d['address'] = d['address'].apply(lambda x: x.replace(' fshr',' foreshore '))
# d['address'] = d['address'].apply(lambda x: x.replace(' mw',' mews '))
# d['address'] = d['address'].apply(lambda x: x.replace(' s',''))
# d['address'] = d['address'].apply(lambda x: x.replace(' n',' north'))
# d['address'] = d['address'].apply(lambda x: x.replace(' qd',' quadrant'))
# d['address'] = d['address'].apply(lambda x: x.replace(' qy',' quay'))


# In[67]:


d['address'] = d['address'].apply(lambda x: x.split(' ')[0:2])
d['address'] = d['address'].apply(lambda x: x[0] +' '+ x[1])


# In[68]:


# merg2 = merg.dropna(subset=['address','Address'])
# merg2[merg2['Status'] == 'private sale']


# In[69]:


merg = pd.merge(d[['suburb','city','address','status','price']],R_short[['Sub','City','Address','Status','Price']], how = 'outer', left_on = ('city','suburb','address'), right_on =('City','Sub','Address'))
bot = merg[merg['suburb'].isna()].iloc[:,5:].reset_index(drop=True)
top = merg[~merg['suburb'].isna()].iloc[:,0:5]
#bot = merg.iloc[1173:,5:].reset_index(drop = True)
bot.columns=['suburb','city','address','status','price']
auction = pd.concat([top,bot],ignore_index = True, axis = 0)
auction['Timestam'] = today


# In[70]:


auction.to_csv(f'.\Aggregated_files\city_total_{today}.csv',index =False)


# In[71]:


auction['Sold at auction'] = auction['status'].apply(lambda x: 1 if x =='sold at auction' or x == 'sold' else 0 )
auction['sold prior to auction'] = auction['status'].apply(lambda x: 1 if x =='sold prior to auction' else 0 ) 
auction['Pass-in'] =auction['status'].apply(lambda x:1 if x == 'passed in' else 0)
auction['Withdrawn'] = auction['status'].apply(lambda x:1 if x =='withdrawn' else 0)
auction['Auction Total'] = auction['status'].apply(lambda x: 0 if x =='private sale' or x=='private exchange' else 1)
auction['Sold After Auction'] = auction['status'].apply(lambda x:1 if x=='sold after auction' else 0)
auction['Private Sale'] =auction['status'].apply(lambda x: 1 if x =='private sale' or x=='private exchange' else 0 )
auction['Total'] = auction['status'].apply(lambda x: 1 if x else 0)
auction['price'] = auction['price'].fillna(0)
auction['price'] = auction['price'].apply(lambda x:str(x).replace('$',''))
auction['price'] = auction['price'].apply(lambda x: float(x.replace('m',''))*1000000 if x.endswith('m') else x)
auction['price'] = auction['price'].apply(lambda x: float(x.replace('k',''))*1000 if str(x).endswith('k') else x)
auction['price'] = auction['price'].apply(lambda x: 0 if x =='price withheld' or ('bid' in str(x)) else x)
auction['price'] = auction['price'].apply(lambda x: float(x.replace(',','')) if type(x) == str else x)
auction['price']= auction['price'].astype('int')


# In[72]:


ac_agg = auction.groupby(['city'],as_index = False)[['Sold at auction','sold prior to auction','Sold After Auction','Pass-in','Withdrawn','Auction Total','Total','Private Sale','price']].sum()
ac_agg['Auction Cleared'] = ac_agg['Sold at auction']+ac_agg['sold prior to auction']
ac_agg['Auction Uncleared'] = ac_agg['Sold After Auction'] + ac_agg['Pass-in'] + ac_agg['Withdrawn']
ac_agg['Auction Rate'] = ac_agg['Auction Cleared']/ac_agg['Auction Total']*100
ac_agg['Auction Rate'] = round(ac_agg['Auction Rate'],2).astype('str')+'%'
ac_agg['Total Clearance'] = (ac_agg['Auction Cleared'] + ac_agg['Private Sale'])/ac_agg['Total']*100
ac_agg['Total Clearance'] =round(ac_agg['Total Clearance'],2).astype('str')+'%'
ac_agg.set_index('city',inplace=True)
ac_agg = ac_agg[['Sold at auction','sold prior to auction','Auction Cleared','Sold After Auction','Pass-in','Withdrawn','Auction Uncleared','Auction Total','Auction Rate','Private Sale','Total','Total Clearance','price']]
ac_agg['date'] = today
ac_agg.to_csv(f'.\YH_AC\YH_AC_{today}.csv')


# In[73]:


ac_agg


# In[59]:



# clearence = pd.concat([top,bot],ignore_index = True, axis = 0)
# clearence['Sold'] = clearence['status'].apply(lambda x: 1 if x == 'sold' or x == 'sold at auction' or x=='sold prior to auction' or x == 'private sale'  or x == 'private exchange' else 0)
# clearence = clearence.groupby(['city'],as_index = False)[['Sold','address']].agg(sold=("Sold","sum"),total=('address','count'))
# clearence['Clearence Rate'] = clearence['sold']/clearence['total']*100





