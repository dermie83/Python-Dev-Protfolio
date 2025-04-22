# -*- coding: utf-8 -*-
"""
Created on Wed Mar  9 12:03:06 2022
Update version on the 23/6/23

@author: madsend

Company: An Post

Depatment: Tranport (DMC)
"""
import pandas as pd
#import numpy as np
from datetime import date, timedelta, datetime


def prepRawData(TSDataFile):
    
    data = pd.DataFrame(TSDataFile)
    data['Stop Geofence'] = data['Stop Geofence'].str.strip()
    data['Driver'] = data['Driver'].fillna('none').str.strip()
    data['Vehicle'] = data['Vehicle'].str.strip()
    data['Vehicle'] = data['Vehicle'].str.replace('LT414B-Tacho Test','LT414B')
    # data['Vehicle'] = data['Vehicle'].str.replace('DT57S - TRAILER','DT57S')
    data['Vehicle'] = data['Vehicle'].str.replace('LT429D-HVO Trial','LT429D')
   
    data['Idle Time (seconds)'].replace({"--": 0}, inplace=True)
    data = data.drop_duplicates()

    return data

def prepDateTime(prepedTSData):
    
    prepedTSData["Date"] = prepedTSData['Arrival Time'].dt.strftime('%Y:%m:%d')
    prepedTSData["Date"] = pd.to_datetime(prepedTSData['Date'], format='%Y:%m:%d')
    
    prepedTSData["Time"] = prepedTSData['Arrival Time'].dt.strftime('%H:%M:%S')
    prepedTSData["Time"] = pd.to_datetime(prepedTSData['Time'], format='%H:%M:%S')
    
    prepedTSData['Weekday'] = prepedTSData['Date'].dt.dayofweek
    prepedTSData['Weekday_name'] = prepedTSData['Date'].dt.day_name()
    prepedTSData['Week'] = prepedTSData['Date'].dt.isocalendar().week
    
    return prepedTSData


def calcFuelCost(dateTimeFile):
    
    travel_metrics = dateTimeFile.groupby(['Weekday_name','Week','Date','Vehicle'],
                                                as_index=False)[['Travel Time (seconds)','Idle Time (seconds)','Distance (km)', 
                                                                 'Time There (seconds)']].sum()
    travel_metrics.set_index("Weekday_name", inplace = True)
        
    travel_metrics['Today'] =  date.today()
    travel_metrics['Todays_Week'] = travel_metrics['Today'].apply(lambda x:x.isocalendar()[1])
    
    # Fuel prices are retrieved from the AA Ireland Fuel Price Figures http://pumps.ie/
    
    # Jan 2023
    travel_metrics.loc[(travel_metrics['Week'] >= 1) & (travel_metrics['Week'] <= 5) ,'Price of Diesel'] = 1.7
    # Feb 2023
    travel_metrics.loc[(travel_metrics['Week'] > 5) & (travel_metrics['Week'] <= 9) ,'Price of Diesel'] = 1.7
    # # Mar 2023
    travel_metrics.loc[(travel_metrics['Week'] > 9) & (travel_metrics['Week'] <= 10) ,'Price of Diesel'] = 1.65
    # # Mar 2023
    travel_metrics.loc[(travel_metrics['Week'] > 10) & (travel_metrics['Week'] <= 13) ,'Price of Diesel'] = 1.6
    # # Apr 2023
    travel_metrics.loc[(travel_metrics['Week'] > 13) & (travel_metrics['Week'] <= 18) ,'Price of Diesel'] = 1.55
    # # May 2023
    travel_metrics.loc[(travel_metrics['Week'] > 18) & (travel_metrics['Week'] <= 20) ,'Price of Diesel'] = 1.5
    # # May 2023
    travel_metrics.loc[(travel_metrics['Week'] > 20) & (travel_metrics['Week'] <= 23) ,'Price of Diesel'] = 1.45
    # # Aug 2023
    travel_metrics.loc[(travel_metrics['Week'] > 23) & (travel_metrics['Week'] <= 33) ,'Price of Diesel'] = 1.6
    # # Sep 2023
    travel_metrics.loc[(travel_metrics['Week'] > 33) & (travel_metrics['Week'] <= 39) ,'Price of Diesel'] = 1.7
    # # Oct 2023
    travel_metrics.loc[(travel_metrics['Week'] > 39) & (travel_metrics['Week'] <= 44) ,'Price of Diesel'] = 1.75
    # # Nov 2023
    travel_metrics.loc[(travel_metrics['Week'] > 44) & (travel_metrics['Week'] <= 53) ,'Price of Diesel'] = 1.8
    # # Dec 2023
    
    #travel_metrics.loc[(travel_metrics['Week'] > 1) & (travel_metrics['Week']  <= travel_metrics['Todays_Week']) ,'Price of Diesel'] = 1.7
    travel_metrics.loc[(travel_metrics['Date'] == travel_metrics['Today']) ,'Price of Diesel'] = 1.8
    
    # make a new filter with network day
    travel_metrics = travel_metrics.drop(['Today','Todays_Week'], axis =1)
    
    return travel_metrics


def calcTotalRunningCosts(fuelCost, fleet):
    
    mergedData = fuelCost.merge(fleet, left_on = 'Vehicle', right_on = 'FLEET_NUMBER', how = 'left' )
    
    fuelCostCalc = mergedData['Price of Diesel']*mergedData['Distance (km)']*mergedData['Avg ltr/100km']
    mergedData['Travel Fuel Cost (Approx.)'] = fuelCostCalc
    
    idlingCost = mergedData['Price of Diesel']*(mergedData['Idle Time (seconds)']/(3600))*mergedData['Avg Idling ltr/hr']
    mergedData['Idling Cost (Approx.)'] = idlingCost
    
    totalFuelCost = mergedData['Travel Fuel Cost (Approx.)'] + mergedData['Idling Cost (Approx.)']
    mergedData['Total Fuel Cost (Approx.)'] = totalFuelCost
    
    time_there = mergedData['Time There (seconds)']/(3600)
    mergedData['Time On Site hrs'] = time_there
    
    ############################################################ get yesterday and yesterday last week metrics ##########################
    today = datetime.today()
    minus_1_day = timedelta(days=1)
    mergedData['Yesterday'] = today - minus_1_day
    mergedData['Yesterday'] = mergedData['Yesterday'].dt.strftime('%d-%m-%Y')
    mergedData['Yesterday'] = pd.to_datetime(mergedData['Yesterday'], format='%d-%m-%Y') 
    
    minus_8_days = timedelta(days=8)
    mergedData['Yesterday Last Week'] = today - minus_8_days
    mergedData['Yesterday Last Week'] = mergedData['Yesterday Last Week'].dt.strftime('%d-%m-%Y')
    mergedData['Yesterday Last Week'] = pd.to_datetime(mergedData['Yesterday Last Week'], format='%d-%m-%Y') 
    
    mergedData.loc[(mergedData["Yesterday"] == mergedData["Date"], 'Yesterday Distance')] = mergedData['Distance (km)']
    mergedData.loc[(mergedData["Yesterday Last Week"] == mergedData["Date"], 'Yesterday Last Week Distance')] = mergedData['Distance (km)']
    
    mergedData.loc[(mergedData["Yesterday"] == mergedData["Date"], 'Yesterday Idling Costs')] = mergedData['Idling Cost (Approx.)']
    mergedData.loc[(mergedData["Yesterday Last Week"] == mergedData["Date"], 'Yesterday Last Week Idling Costs')] = mergedData['Idling Cost (Approx.)']
    
    mergedData.loc[(mergedData["Yesterday"] == mergedData["Date"], 'Yesterday Total Fuel Costs')] = mergedData['Total Fuel Cost (Approx.)']
    mergedData.loc[(mergedData["Yesterday Last Week"] == mergedData["Date"], 'Yesterday Last Week Total Fuel Costs')] = mergedData['Total Fuel Cost (Approx.)'] 
    
    return mergedData


def getVehicleUtil(totalRunningCosts):
    #Convert to List
   vehicle_list = totalRunningCosts['Vehicle'].unique().tolist()
   
   
   min_date_range = totalRunningCosts['Date'].min()
   max_date_range = totalRunningCosts['Date'].max()

   date_range = pd.date_range(start=min_date_range, end=max_date_range).tolist()

   # making pairs
   result = [(i, j) for i in vehicle_list for j in date_range if i != j]
   df = pd.DataFrame(result)
   
   df.columns=["Vehicle", 'Date']
   
   merge_df = totalRunningCosts.merge(df, left_on = ['Vehicle','Date'] , right_on = ['Vehicle','Date'], how = 'right' )
   merge_df['Weekday'] = merge_df['Date'].dt.day_name()

   merge_df.loc[pd.isnull(merge_df["Week"]), 'Fleet Utilisation'] = 'Not Utilised or NRU'
  
   merge_df.loc[(merge_df["Distance (km)"] < 2), 'Fleet Utilisation'] = 'UnderUtilised'
   
   merge_df["Fleet Utilisation"].fillna('Utilised', inplace=True)
   
   merge_df['Count'] = 1
   merge_df.loc[(merge_df['Fleet Utilisation'] == 'Not Utilised or NRU') | (merge_df['Fleet Utilisation'] == 'UnderUtilised'),  'Total Vehicle Unused'] = 1
   merge_df.loc[(merge_df['Fleet Utilisation'] == 'Utilised'), 'Total Vehicle Unused'] = 0 
  
   return merge_df
 

def getTotalTimeInService(prepedTSData, fleet, *serviceStationList):
    
    for _ in serviceStationList:
        timeInServiceDF = prepedTSData.loc[prepedTSData.apply(lambda x: x['Stop Geofence'] in _ , axis=1 )]
        
        timeInServiceDF = timeInServiceDF.groupby(['Vehicle', 'Stop Geofence', 'Date',],
                                                    as_index=False)[['Time There (seconds)']].sum()
        
        timeInServiceDF = timeInServiceDF.merge(fleet, left_on = 'Vehicle', right_on = 'FLEET_NUMBER', how = 'left' )
        
        #print(timeInServiceDF.columns)
        
        timeInServiceDF = timeInServiceDF.drop(['FLEET_NUMBER', 'REG_NUMBER', 'MODEL', 'MAKE', 'VEHICLE_TYPE',
                                                'FUEL_USED', 'Tonnage', 'STATUS_CODE', 'FINANCE_METH',
                                                'Avg ltr/100km', 'Avg Idling ltr/hr', 'Stationary Daily Fixed Cost',	
                                                'Driver hourly rate', 'Cost per item to B.E. @ 3.5 cent'], axis=1)
        
        timeInServiceDF.to_csv('Service Time.csv')
        
    return timeInServiceDF


serviceStationList = ["Dennehy's Limerick"]


# this file is already prepared in excel
sheet = ['Jan_2023']
mdn_fleet_file = pd.read_excel(r'Fleet_2023.xlsx', f"{sheet[2]}")
df_mdn = pd.DataFrame(mdn_fleet_file)

sheets = ['Jan 2023']
distance_file = pd.read_excel(r'Travel and Stop Master.xlsx', sheet_name = sheets[1])

distance_prep = prepRawData(distance_file)

datePrep = prepDateTime(distance_prep)

timeInService = getTotalTimeInService(datePrep, df_mdn, serviceStationList)

fuel_costs_calc = calcFuelCost(datePrep)

fuel_cost_metrics = calcTotalRunningCosts(fuel_costs_calc, df_mdn)

util_metrics = getVehicleUtil(fuel_cost_metrics)


final_data = util_metrics.merge(df_mdn, left_on = 'Vehicle', right_on = 'FLEET_NUMBER', how = 'left' )


final_data.rename(columns = {'MODEL_y':'MODEL', 'DEPOT_y':'DEPOT',	'STATUS_CODE_y':'STATUS_CODE', 
                              'Tonnage_y':'Tonnage',	'VEHICLE_TYPE_y':'VEHICLE_TYPE', 'FUEL_USED_y':'FUEL_USED'}, inplace = True)

final_data = final_data.set_index('Vehicle')

final_data = final_data[['MODEL', 'DEPOT', 'STATUS_CODE', 'Tonnage', 'VEHICLE_TYPE','FUEL_USED', 
                          'Date', 'Travel Time (seconds)', 'Idle Time (seconds)',
                          'Distance (km)', 'Time There (seconds)', 'Travel Fuel Cost (Approx.)',
                          'Idling Cost (Approx.)', 'Total Fuel Cost (Approx.)',
                          'Time On Site hrs',
                          'Weekday', 'Fleet Utilisation', 'Count', 'Total Vehicle Unused',
                          'Yesterday Distance', 'Yesterday Last Week Distance',
                          'Yesterday Idling Costs', 'Yesterday Last Week Idling Costs',
                          'Yesterday Total Fuel Costs', 'Yesterday Last Week Total Fuel Costs']]
          
                                                  
final_data.to_csv('Network Metrics.csv')



