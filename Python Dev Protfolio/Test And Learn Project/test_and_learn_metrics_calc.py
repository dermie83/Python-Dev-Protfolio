# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 13:50:50 2023

@author: madsend
"""

import pandas as pd

data = pd.read_csv(r'file_1.csv')
dataDF = pd.DataFrame(data)

sheet = ['FLEET']
fleet = pd.read_excel(r'daily_snapshot_fleet.xlsx', sheet_name = sheet[0])
fleet_df = pd.DataFrame(fleet)
#print(dataDF)

data_homeDF = dataDF.loc[(dataDF['Route Stage'] == 'Home Garaging')]

data_shiftEndDF = dataDF.loc[(dataDF['Route Stage'] == 'End Of Shift')]

data_startDF = dataDF.loc[(dataDF['Route Stage'] == 'Start Of Route')]
df_start = data_startDF.groupby('Vehicle Number', sort=False).apply(lambda x: x[x['Date Time'] == x['Date Time'].min()]).reset_index(drop=True)


data_endDF = dataDF.loc[(dataDF['Route Stage'] == 'End Of Route')]
df_end = data_endDF.groupby('Vehicle Number', sort=False).apply(lambda x: x[x['Date Time'] == x['Date Time'].max()]).reset_index(drop=True)


dataframes = [df_start, df_end, data_homeDF, data_shiftEndDF]
dataframes1 = pd.concat(dataframes).reset_index(drop=True)

df = dataframes1.drop(['sequence','Unnamed: 0'], axis=1)

df['sequence'] = df.sort_values(by=['Vehicle Number', 'Date Time']).groupby(['Vehicle Number']).cumcount() + 1

df = [x for _, x in df.groupby('sequence')]

merge_fleet = pd.concat([x.set_index('Vehicle Number') for x in df],axis = 1,keys=range(len(df)))

merge_fleet.columns = merge_fleet.columns.map('{0[1]}_{0[0]}'.format)



Total_Duty_Time1 =  merge_fleet['Accumulated Time (Seconds)_1'] - merge_fleet['Accumulated Time (Seconds)_0']
merge_fleet['Duty_Time1'] = Total_Duty_Time1/3600

Total_Duty_Time2 =  merge_fleet['Accumulated Time (Seconds)_2'] - merge_fleet['Accumulated Time (Seconds)_1']
merge_fleet['Duty_Time2'] = Total_Duty_Time2/3600

Total_Duty_Time3 =  merge_fleet['Accumulated Time (Seconds)_3'] - merge_fleet['Accumulated Time (Seconds)_2']
merge_fleet['Duty_Time3'] = Total_Duty_Time3/3600

time = merge_fleet['Accumulated Time (Seconds)_0']

merge_fleet.loc[(merge_fleet['Route Stage_0'] == 'End Of Shift'),'Duty_Time1' ] = time/3600
merge_fleet.loc[(merge_fleet['Route Stage_1'] == 'End Of Route'),'Duty_Time2' ] = 0
merge_fleet.loc[(merge_fleet['Route Stage_2'] == 'End Of Route'),'Duty_Time1' ] = 0
merge_fleet.loc[(merge_fleet['Route Stage_2'] == 'End Of Route'),'Duty_Time3' ] = 0

merge_fleet.loc[(merge_fleet['Duty_Time2'].isna() ),'Duty_Time2' ] = 0
merge_fleet.loc[(merge_fleet['Duty_Time3'].isna() ),'Duty_Time3' ] = 0

Total_Duty_Time =  merge_fleet['Duty_Time1'] + merge_fleet['Duty_Time2'] + merge_fleet['Duty_Time3']
merge_fleet['Total Route Time'] = Total_Duty_Time




Total_Duty_Distance1 =  merge_fleet['Daily accumulated distance_1'] - merge_fleet['Daily accumulated distance_0']
merge_fleet['Duty_Distance1'] = Total_Duty_Distance1

Total_Duty_Distance2 =  merge_fleet['Daily accumulated distance_2'] - merge_fleet['Daily accumulated distance_1']
merge_fleet['Duty_Distance2'] = Total_Duty_Distance2

Total_Duty_Distance3 =  merge_fleet['Daily accumulated distance_3'] - merge_fleet['Daily accumulated distance_2']
merge_fleet['Duty_Distance3'] = Total_Duty_Distance3


distance = merge_fleet['Daily accumulated distance_0']

merge_fleet.loc[(merge_fleet['Route Stage_0'] == 'End Of Shift'),'Duty_Distance1' ] = distance
merge_fleet.loc[(merge_fleet['Route Stage_1'] == 'End Of Route'),'Duty_Distance2' ] = 0
merge_fleet.loc[(merge_fleet['Route Stage_2'] == 'End Of Route'),'Duty_Distance1' ] = 0
merge_fleet.loc[(merge_fleet['Route Stage_2'] == 'End Of Route'),'Duty_Distance3' ] = 0

merge_fleet.loc[(merge_fleet['Duty_Distance2'].isna() ),'Duty_Distance2' ] = 0
merge_fleet.loc[(merge_fleet['Duty_Distance3'].isna() ),'Duty_Distance3' ] = 0

Total_Duty_Distance =  merge_fleet['Duty_Distance1'] + merge_fleet['Duty_Distance2'] + merge_fleet['Duty_Distance3']
merge_fleet['Total Route Distance'] = Total_Duty_Distance


merge_fleet['Average Route Speed'] = Total_Duty_Distance/Total_Duty_Time

merge_fleet = merge_fleet.drop(['sequence_0', 'sequence_1','sequence_2','sequence_3'], axis=1)

fleet_dff = fleet_df.drop(['ACTIVITY','ODOMETER_DAT','ODOMETER_DIS','ADMINISTOR','DEMANDACTIVITY'], axis=1)	

merged = fleet_dff.merge(merge_fleet, left_on = ['FLEET_NUMBER'] , right_on = ['Vehicle Number'], how = 'left' )


excelfilename = "fleet_summary_metrics_output.csv"
merged.to_csv(excelfilename)