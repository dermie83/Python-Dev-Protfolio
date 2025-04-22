# -*- coding: utf-8 -*-
"""
Created on Thurs Jun 1 2023
Update version on the 23/6/23

@author: madsend

Company: An Post

Depatment: Tranport (DMC)
"""
import pandas as pd
from datetime import timedelta, datetime
from locations1 import locations_df


def prepRawData(rawAlertDataFile):
    
    """ This function is to 'Trim" the alert data, drop duplicates 
        and replace 'Null' values in the "Driver" column with 'none' """
    
    data = pd.DataFrame(rawAlertDataFile)
    data['Driver'] = data['Driver'].fillna('none').str.strip()
    data['Vehicle'] = data['Vehicle'].str.strip()
    data['Vehicle'] = data['Vehicle'].str.replace('LT414B-Tacho Test','LT414B')
    # data['Vehicle'] = data['Vehicle'].str.replace('DT57S - TRAILER','DT57S')
    data['Vehicle'] = data['Vehicle'].str.replace('LT429D-HVO Trial','LT429D')
    data['Alert Value'] = data['Alert Value'].str.strip()
    data = data.drop_duplicates(subset = ['Vehicle','Driver','Alert Value','Last triggered date & time'], keep='last')
    
    return data

    
def prepDateTime(prepedAlertData):
    
    prepedAlertData["Date"] = prepedAlertData['Last triggered date & time'].dt.strftime('%Y:%m:%d')
    prepedAlertData["Date"] = pd.to_datetime(prepedAlertData['Date'], format='%Y:%m:%d')
    
    prepedAlertData["Time"] = prepedAlertData['Last triggered date & time'].dt.strftime('%H:%M:%S')
    prepedAlertData["Time"] = pd.to_datetime(prepedAlertData['Time'], format='%H:%M:%S')
    
    prepedAlertData['Weekday'] = prepedAlertData['Date'].dt.dayofweek
    prepedAlertData['Weekday_name'] = prepedAlertData['Date'].dt.day_name()
    prepedAlertData['Week'] = prepedAlertData['Date'].dt.isocalendar().week
    
    return prepedAlertData


def calcPingCount(rawAlertDataFile, locations):
    
    today=datetime.today()
    minus_1_day = timedelta(days=1)
    minus_8_days = timedelta(days=8)
    
    rawAlertDataFile['Count'] = 1
    
    alertValueBingCount = rawAlertDataFile
    alertValueBingCount['Yesterday'] = today - minus_1_day
    alertValueBingCount['Yesterday'] = alertValueBingCount['Yesterday'].dt.strftime('%d-%m-%Y')
    alertValueBingCount['Yesterday'] = pd.to_datetime(alertValueBingCount['Yesterday'], format='%d-%m-%Y') 

    alertValueBingCount['Yesterday Last Week'] = today - minus_8_days
    alertValueBingCount['Yesterday Last Week'] = alertValueBingCount['Yesterday Last Week'].dt.strftime('%d-%m-%Y')
    alertValueBingCount['Yesterday Last Week'] = pd.to_datetime(alertValueBingCount['Yesterday Last Week'], format='%d-%m-%Y') 

    alertValueBingCount.loc[(alertValueBingCount["Yesterday"] == alertValueBingCount["Date"], 'Yesterdays Total')] = alertValueBingCount["Count"]
    alertValueBingCount.loc[(alertValueBingCount["Yesterday Last Week"] == alertValueBingCount["Date"], 'Yesterday Last Week Total')] = alertValueBingCount["Count"]
    
    alertValueBingCount.loc[(alertValueBingCount["Alert Value"].str.contains("Entered"),'Entered')] = 'Entered'
    
    alertValueBingCount['Entered'] = alertValueBingCount['Entered'].fillna('none')
    
    alertValueBingCount = alertValueBingCount.loc[(alertValueBingCount['Entered'] != "none")]
    
    locationsMerged = alertValueBingCount.merge(locations, left_on = 'Alert Value', right_on = 'Alert Value', how = 'left' )
    locationsMerged = locationsMerged.drop(['lat_radians_loc','long_radians_loc','Yesterday',
                                            'Yesterday Last Week', 'Entered', 'Active in Network'], axis =1)
    
    locationsMerged.to_csv('Alert Value Metrics 2023.csv')
    #print(locationsMerged.columns)
    
    return locationsMerged


def calcRollingAvg(locationsMerged, fleet):
   
    dfs = locationsMerged.merge(fleet, left_on = 'Vehicle', right_on = 'FLEET_NUMBER', how = 'left' )
   
    
    # Count Pings/Traffic by Alert Value
    df = dfs.groupby(['Alert Value','VEHICLE_TYPE','Location', 'Week']).size().reset_index(name = 'Count')
    df = df.set_index('Week').groupby(['Alert Value','VEHICLE_TYPE','Location']).expanding().mean()
    df = df.reset_index().rename(columns={'Count': 'Avg Ping Count'})
    df.to_csv('Rolling AvG Mean by Alert Value.csv')
    
    return df

    

    
sheet = ['Jan_2023']
mdn_fleet_file = pd.read_excel(r'Fleet_2023.xlsx', f"{sheet[3]}")
df_mdn = pd.DataFrame(mdn_fleet_file)

data_sheets = ['Jan 2023']
raw_file = pd.read_excel(r'Alert Data.xlsx', f"{data_sheets[0]}")
raw_data_1 = prepRawData(raw_file)
prepDate = prepDateTime(raw_data_1)
alertValueMetrics = calcPingCount(prepDate, locations_df)
rollingMean = calcRollingAvg(alertValueMetrics, df_mdn)
   
                




