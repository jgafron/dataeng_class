#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import uuid

path = 'C:/Users/Mr. Grinch/Downloads/Data/acs2017_census_tract_data.csv'

df = pd.read_csv(path, usecols=['State', 'County', 'TotalPop', 'Poverty', 'IncomePerCap'])

# Calculate total income for each region
df['povertyWeighted'] = df['Poverty'] * df['TotalPop']
df['TotalIncome'] = df['IncomePerCap'] * df['TotalPop']


county_info1 = df.groupby('County').agg({
    'State': 'first',
    'TotalPop': 'sum',
    'Poverty': 'sum',
    'TotalIncome': 'sum',
    'povertyWeighted' : 'sum'
}).reset_index()

county_info1['povertyPercent'] = county_info1['povertyWeighted'] / county_info1['TotalPop']

county_info1['IncomePerCap'] = county_info1['TotalIncome'] / county_info1['TotalPop']

county_info1 = county_info1.drop(columns=['TotalIncome', 'povertyWeighted', 'Poverty'])

county_info2 = county_info1.sort_values(by='State')

county_info3 = county_info2.assign(ID=lambda x: [uuid.uuid4() for _ in range(len(x))])


county_info3


# In[2]:


def search_county(county_name):
    return county_info3[county_info3['County'] == county_name]

loudoun_info = search_county('Loudoun County')
print(loudoun_info)

washington_info = search_county('Washington County')
print(washington_info)

harlan_info = search_county('Harlan County')
print(harlan_info)

malheur_info = search_county('Malheur County')
print(malheur_info)


# In[3]:


county_info4 = county_info3.sort_values(by='County')
county_info4


# In[4]:


import pandas as pd
import uuid

covid_path = 'C:/Users/Mr. Grinch/Downloads/Data/COVID_county_data.csv'

covid_df = pd.read_csv(covid_path, usecols=['state', 'date', 'county', 'cases', 'deaths'])

# Convert 'date' column to datetime format
covid_df['date'] = pd.to_datetime(covid_df['date'])

covid_df['month'] = covid_df['date'].dt.month_name()
covid_df['year'] = covid_df['date'].dt.year

covid_monthly = covid_df.groupby(['county', 'month', 'year']).agg({
    'state': 'first',
    'cases': lambda x: x.iloc[-1] - x.iloc[0],
    'deaths': lambda x: x.iloc[-1] - x.iloc[0],
}).reset_index()

covid_monthly['county'] = covid_monthly['county'].str.split().str[0].str.lower()

# Convert county names to lowercase and match only the first word
county_info3['County'] = county_info3['County'].str.split().str[0].str.lower()

covid_monthly = pd.merge(covid_monthly, county_info3[['County', 'ID']], left_on='county', right_on='County', how='left')

# Drop the 'County' column as it's no longer needed
covid_monthly = covid_monthly.drop(columns=['County'])

covid_monthly


# In[5]:



def search_county(county_name):
    return covid_monthly[covid_monthly['county'] == county_name]

malheur_info = search_county('malheur')
print(malheur_info)


# In[6]:


import pandas as pd

# Group by 'County' in county_info3 DataFrame to get population, poverty, and income data
county_summary = county_info3.groupby('County').agg({
    'ID': 'first',  
    'TotalPop': 'first',  
    'povertyPercent': 'first', 
    'IncomePerCap': 'first'  
}).reset_index()


covid_summary = covid_monthly.groupby('county').agg({
    'cases': 'sum',  # Total cases
    'deaths': 'sum'  # Total deaths
}).reset_index()

COVID_summary = pd.merge(county_summary, covid_summary, left_on='County', right_on='county', how='left')

COVID_summary['TotalCasesPer100K'] = ((COVID_summary['cases'] / COVID_summary['TotalPop']) * 100000).round(2)
COVID_summary['TotalDeathsPer100K'] = ((COVID_summary['deaths'] / COVID_summary['TotalPop']) * 100000).round(2)

# Drop the redundant 'county' column
COVID_summary.drop(columns=['county'], inplace=True)

COVID_summary


# In[7]:



# Function to search for specific counties
def search_county(county_name):
    return COVID_summary[COVID_summary['County'] == county_name]

# Search for Shelby County
malheur_info = search_county('malheur')
print(malheur_info)

washington_info = search_county('washington')
print(washington_info)

loudoun_info = search_county('loudoun')
print(loudoun_info)

harlan_info = search_county('harlan')
print(harlan_info)


# In[ ]:




