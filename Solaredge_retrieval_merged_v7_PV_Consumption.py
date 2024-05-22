#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 13 22:03:41 2024

@author: mayk
Changes: altered script to save both PV production and local consumption at site measured by the SolarEdge grid meter
"""

import pandas as pd
import solaredge
import time
import functools as ft

s = solaredge.Solaredge("9P6QJRA3SZED0VXNARKL8F4R8XPSU1GK")
site_id = 1041749

# Edit this date range as you see fit
# If querying at the maximum resolution of 15 minute intervals, the API is limited to queries of a month at a time
# This script queries one day at a time, with a one-second pause per day that is polite but probably not necessary
day_list = pd.date_range(start="2020-01-01",end="2020-12-31")
day_list = day_list.strftime('%Y-%m-%d')



# energy_df_list = []
# for day in day_list:
#     temp_1 = s.get_energy_details(site_id,day+' 00:00:00',day +  ' 23:59:59',time_unit='QUARTER_OF_AN_HOUR')
#     temp_df = pd.DataFrame(temp_1['energyDetails']['meters'][0]['values'])
#     energy_df_list.append(temp_df)
#     #time.sleep(1)
# energy_df = pd.concat(energy_df_list)
# energy_df.columns = ['date','ßPV_energy']



list_Production      = []
list_Consumption     = []


for day in day_list:
 
    temp = s.get_power_details( site_id, day+' 00:00:00', day +' 23:59:59', meters='Production')
    temp_Production = pd.DataFrame(temp['powerDetails']['meters'][0]['values'])
    list_Production.append(temp_Production)
    time.sleep(0.1)
    
    temp = s.get_power_details( site_id, day+' 00:00:00', day +' 23:59:59', meters='Consumption')
    temp_Consumption = pd.DataFrame(temp['powerDetails']['meters'][0]['values'])
    list_Consumption.append(temp_Consumption)
    time.sleep(0.1)

    
prod_df = pd.concat(list_Production)
prod_df.columns = ['date','Production']

cons_df = pd.concat(list_Consumption)
cons_df.columns = ['date','Consumption']


#merged = pd.merge(energy_df,prod_df,cons_df)

#merged1 = pd.merge(energy_df,prod_df,on='date')
#merged2 = pd.merge(merged1,cons_df,on='date')
#merged = merged2

#merged = pd.merge( pd.merge(pd.merge( pd.merge(prod_df,cons_df,on='date') ,self_df ,on='date') , impo_df,on='date')  , vijf_df,on='date')

#merged = pd.merge(prod_df,cons_df)

dfs = [prod_df, cons_df]

df_final = ft.reduce(lambda left, right: pd.merge(left, right, on='date'), dfs)


#df_final.to_csv("SolarEdge_Kr12_xxxx_merged.csv",index=False)
df_final.to_excel('SolarEdge_Kr12_2020_PV_Cons.xlsx', index=False)

# import os
# duration = 1  # seconds
# freq = 440  # Hz
# os.system('play -nq -t alsa synth {} sine {}'.format(duration, freq))


