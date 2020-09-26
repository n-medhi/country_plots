#!/usr/bin/env python
# coding: utf-8

# In[53]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import csv
import json
import requests
from datetime import datetime


# In[54]:


df = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
focus = df.copy().drop(['Lat','Long'], axis=1).set_index(['Country/Region','Province/State'])
confirm = focus.groupby('Country/Region').sum().reset_index()


# In[55]:


do_not_include = ['Antigua and Barbuda', 'Angola', 'Benin', 'Botswana', 
                  'Burundi', 'Cabo Verde', 'Chad', 'Comoros', 
                  'Congo (Brazzaville)', 'Congo (Kinshasa)',"Cote d'Ivoire", 'Central African Republic',
                  'Diamond Princess', 'Equatorial Guinea',
                  'Eritrea', 'Eswatini',   'Gabon', 
                  'Gambia', 'Ghana', 'Grenada', 'Guinea', 'Guinea-Bissau',
                  'Guyana', 'Lesotho', 'Liberia', 'Libya', 'Madagascar',
                  'Malawi', 'Maldives', 'Mauritania', 'Mozambique',
                  'MS Zaandam', 'Namibia', 'Nicaragua', 'Papua New Guinea',
                  'Rwanda',   'Saint Lucia', 
                  'Saint Vincent and the Grenadines', 'Sao Tome and Principe',
                  'Seychelles', 'Sierra Leone', 'South Sudan', 'Suriname', 'Syria', 
                  'Tanzania',   'Togo', 'Uganda', 'West Bank and Gaza',
                  'Western Sahara', 'Yemen', 'Zambia', 'Zimbabwe']


# In[56]:


focus


# In[57]:


## replacing 0 total cases with nan
#confirm.replace(0, np.nan, inplace=True)


# In[58]:


confirm


# In[59]:


# convert "pivoted" data to "long form"
data = pd.melt(confirm, id_vars=['Country/Region'], var_name='date', value_name='cases')

data = data.rename(columns = {'Country/Region':'country'})

# convert date column
data['date'] = pd.to_datetime(data['date'], format= '%m/%d/%y')


# In[60]:


data


# In[61]:


# pivot data with countries as columns
pivot_cases = pd.pivot_table(data, index = "date", columns = "country", values= "cases")

# drop countries listed above
pivot_cases = pivot_cases.drop(columns=do_not_include)


# In[62]:


pivot_cases


# In[63]:


# new dataframe to store "daily new cases"
pivot_newcases = pivot_cases.copy()

# calculate "daily new cases"
for column in pivot_newcases.columns[0:]:
    DailyNewCases = column
    pivot_newcases[DailyNewCases] = pivot_newcases[column].diff()


# In[64]:


# fill NaN in pivot_newcases (first row) with values from pivot_cases
pivot_newcases.fillna(pivot_cases, inplace=True)


# In[65]:


pivot_newcases


# In[66]:


# replace negative daily values by setting 0 as the lowest value
pivot_newcases = pivot_newcases.clip(lower=0)


# In[67]:


# new dataframe to store "avg new cases"
pivot_avgnewcases = pivot_newcases.copy()

# calculate 7-day averages of new cases
for column in pivot_avgnewcases.columns[0:]:
    DaySeven = column
    pivot_avgnewcases[DaySeven] = pivot_avgnewcases[column].rolling(window=7, center=False).mean()


# In[68]:


# fill NaN in pivot_avgnewcases (first 6 rows) with values from pivot_newcases
pivot_recentnew = pivot_avgnewcases.fillna(pivot_newcases)


# In[69]:


pivot_recentnew


# In[70]:


# new dataframe to store "avg new cases" with centered average
pivot_avgnewcases_center = pivot_newcases.copy()

# calculate 7-day averages of new cases with centered average
for column in pivot_avgnewcases_center.columns[0:]:
    DaySeven = column
    pivot_avgnewcases_center[DaySeven] = pivot_avgnewcases_center[column].rolling(window=7, min_periods=4, center=True).mean()


# In[71]:


pivot_avgnewcases_center


# In[72]:


## new dataframe to store "avg new cases" with centered average
#pivot_recentnew_peaktodate = pivot_recentnew.copy()

## calculate 7-day averages of new cases with centered average
#for column in pivot_recentnew_peaktodate.columns[0:]:
#    DaySeven = column
#    pivot_recentnew_peaktodate[DaySeven] = pivot_recentnew_peaktodate[column].cummax()


# In[73]:


#pivot_recentnew_peaktodate


# In[74]:


# new dataframe to store peak 7-day average to date 
pivot_recentnew_peaktodate = pivot_recentnew.cummax()


# In[75]:


pivot_recentnew_peaktodate


# In[76]:


# reset indexes of "pivoted" data
pivot_cases = pivot_cases.reset_index()
pivot_newcases = pivot_newcases.reset_index()
pivot_recentnew = pivot_recentnew.reset_index()
pivot_avgnewcases_center = pivot_avgnewcases_center.reset_index()
pivot_recentnew_peaktodate = pivot_recentnew_peaktodate.reset_index()


# In[77]:


# convert "pivot" of total cases to "long form"
country_cases = pd.melt(pivot_cases, id_vars=['date'], var_name='country', value_name='cases')


# In[78]:


country_cases


# In[79]:


# convert "pivot" of daily new cases to "long form"
country_newcases = pd.melt(pivot_newcases, id_vars=['date'], var_name='country', value_name='new_cases')


# In[80]:


country_newcases


# In[81]:


# convert "pivot" of recent new cases to "long form" (7-day avg w first 6 days from "new cases")
country_recentnew = pd.melt(pivot_recentnew, id_vars=['date'], var_name='country', value_name='recent_new')


# In[82]:


country_recentnew


# In[83]:


# convert "pivot" of centered average new cases to "long form"
country_avgnewcases_center = pd.melt(pivot_avgnewcases_center, id_vars=['date'], var_name='country', value_name='avg_cases')


# In[84]:


country_avgnewcases_center


# In[85]:


# convert "pivot" of centered average new cases to "long form"
country_recentnew_peaktodate = pd.melt(pivot_recentnew_peaktodate, id_vars=['date'], var_name='country', value_name='peak_recent_new')


# In[86]:


country_recentnew_peaktodate


# In[87]:


# merge the 5 "long form" dataframes based on index
country_merge = pd.concat([country_cases, country_newcases, country_avgnewcases_center, country_recentnew, country_recentnew_peaktodate], axis=1)


# In[88]:


# NOTE:
# original code uses integer from latest 7-day average in country color logic

# take integer from "recent_new"
country_merge['recent_new_int'] = country_merge['recent_new'].astype(int)


# In[89]:


# remove duplicate columns
country_merge = country_merge.loc[:,~country_merge.columns.duplicated()]


# In[90]:


country_merge


# In[91]:


## UPDATE 9/25/20 - modified green logic due to quirk caused by original logic on countries page
## original logic caused Uruguay with avg ~16 cases to appear red because 16 > 50% of its low peak of 24

## Orignial green logic:
## if state_color_test['recent_new_int'] <= n_0*f_0 or state_color_test['recent_new_int'] <= n_0 and state_color_test['recent_new_int'] <= f_0*state_color_test['peak_recent_new']:

#choosing colors
n_0 = 20
f_0 = 0.5
f_1 = 0.2

# https://stackoverflow.com/questions/49586471/add-new-column-to-python-pandas-dataframe-based-on-multiple-conditions/49586787
def conditions(country_merge):
    if country_merge['recent_new_int'] <= n_0:
        return 'green'
    elif country_merge['recent_new_int'] <= 1.5*n_0 and country_merge['recent_new_int'] <= f_0*country_merge['peak_recent_new'] or country_merge['recent_new_int'] <= country_merge['peak_recent_new']*f_1:
        return 'orange'
    else:
        return 'red'

country_merge['color_historical'] = country_merge.apply(conditions, axis=1)


# In[92]:


country_merge


# In[93]:


# dataframe with only the most recent date for each country
# https://stackoverflow.com/questions/23767883/pandas-create-new-dataframe-choosing-max-value-from-multiple-observations
country_latest = country_merge.loc[country_merge.groupby('country').date.idxmax().values]


# In[94]:


country_latest


# In[95]:


# dataframe with just country, total cases, and color
country_total_color = country_latest[['country','cases','color_historical']]

# rename cases to total_cases and color_historical to color for the purpose of merging
country_total_color = country_total_color.rename(columns = {'cases':'total_cases', 'color_historical':'color'})


# In[96]:


country_total_color


# In[97]:


# merging total cases onto the merged dataframe
country_final = country_merge.merge(country_total_color, on='country', how='left')


# In[98]:


## drop rows where cumulative cases is NaN (dates before reported cases)
#country_final = country_final.dropna(subset=['cases']) 


# In[99]:


country_final


# In[100]:


#correcting country names
countryrename = {'Taiwan*' : 'Taiwan',
                 'Korea, South' : 'South Korea',
                 'United Arab Emirates' : 'U.A.E.',
                 'Bosnia and Herzegovina' : 'Bosnia'}

country_final['country'] = country_final['country'].replace(countryrename)


# In[101]:


## Remove the 'cases' column to match format of Era's state result file 
result = country_final[['country','date','new_cases','avg_cases','total_cases','recent_new','color']]

result.to_csv('result.csv', index=False)


# In[102]:


# dataframe with just country and color
country_color = country_total_color[['country','color']]

# creates csv similar to USStateColors.csv
country_color.to_csv('CountryColors.csv', index=False)


# In[103]:


# count number of countries by color by date
color_by_date = pd.crosstab(index = country_final['date'], columns=country_final['color_historical'])


# In[104]:


color_by_date


# In[105]:


color_by_date.to_csv('color_by_date.csv')

