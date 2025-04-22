import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta

# import Track and Trace schedule 
T_and_T = ['schedule']
file = pd.read_excel(r'transport_schedules.xlsx', f"{T_and_T[0]}")
MC = pd.DataFrame(file)

def T_T_schedule(df):
    df['TerminatingTime'] = pd.to_datetime(df['TerminatingTime'], format='%H:%M:%S')
    df['OriginatingTime'] = pd.to_datetime(df['OriginatingTime'], format='%H:%M:%S')
    df['Duration'] = pd.to_datetime(df['Duration'], format='%H:%M:%S')
    df['TerminatingTime'] = df['TerminatingTime'].dt.strftime('%H:%M:%S')
    df['TerminatingTime'] = pd.to_timedelta(df['TerminatingTime'])
    df['OriginatingTime'] = df['OriginatingTime'].dt.strftime('%H:%M:%S')
    df['OriginatingTime'] = pd.to_timedelta(df['OriginatingTime'])
    df['Duration'] = df['Duration'].dt.strftime('%H:%M:%S')
    df['Duration'] = pd.to_timedelta(df['Duration'])
    df=df.drop(['ScheduleType','Active','Last ScheduledDate','Next ScheduledDate','Description'], axis = 1)
    travel_time = df['Duration'] / np.timedelta64(1, 'h')
    df['Scheduled_Duration'] = travel_time.round(2)
    return df
Schedule=T_T_schedule(MC)



# Weekday select (use 'N' instead of 'Y' for Sunday)
Schedule = Schedule[Schedule['Sun'] != 'Y']

# import geofence alerts - preferrably from an API feed verizon provide
geofence_0=pd.read_csv(r'geo_alerts_file.csv')

geo_0 = pd.DataFrame(geofence_0)

verizon_alerts = geo_0


def verizon_data_parse(df):
    df['Actual_Time'] = df['Last triggered date & time']
    df = df.drop(['Last triggered date & time','Driver'],axis=1)

    # We want to split the Exited values from the Entered values

    df['Exited']= df['Alert Value'].str.startswith('Exited')
    origin_site = df[df['Exited'] == True]
    terminal_site = df[df['Exited'] == False]
    
    # Merge dataframes to make single datframe of exited and entered alerts
    
    merge = pd.merge(origin_site,terminal_site, how='inner', on = 'Vehicle')
    merge = merge.drop(['Exited_x','Exited_y'],axis=1)
    origin = merge

    origin = origin.sort_values(by ='Vehicle' )
    origin = origin.reset_index(drop=True)

    origin['Departure_Time'] = pd.to_datetime(origin['Actual_Time_x'],dayfirst=True)
    origin['Arrival_Time'] = pd.to_datetime(origin['Actual_Time_y'],dayfirst=True)

    # format times to days and hours 
    begin_time = origin['Departure_Time']
    end_time = origin['Arrival_Time']
    origin['Actual_Time_Diff'] = origin['Arrival_Time'] - origin['Departure_Time']
    check_days = origin['Actual_Time_Diff'].dt.days
    check_hours = origin['Actual_Time_Diff'] / np.timedelta64(1, 'h')
    origin['days'] = check_days
    origin['hours'] = check_hours.round(2)

    # filter out any days that are minus value as they will be out of scope/illegal argument
    filter_days = origin[origin['days'] == 0]
    dep = filter_days.reset_index(drop=True)

    #format time and dates to enable user to add times for setting Scheduling boundaries
    dep['Departure_Date'] = dep['Departure_Time'].dt.strftime('%Y:%m:%d')
    dep['Departure_Time'] = dep['Departure_Time'].dt.strftime('%H:%M:%S')
    dep['Departure_Date'] = pd.to_datetime(dep['Departure_Date'], format='%Y:%m:%d')
    dep['Departure_Time'] = pd.to_timedelta(dep['Departure_Time'])

    dep['Arrival_Date'] = dep['Arrival_Time'].dt.strftime('%Y:%m:%d')
    dep['Arrival_Time'] = dep['Arrival_Time'].dt.strftime('%H:%M:%S')
    dep['Arrival_Date'] = pd.to_datetime(dep['Arrival_Date'], format='%Y:%m:%d')
    dep['Arrival_Time'] = pd.to_timedelta(dep['Arrival_Time'])
    return dep

dep = verizon_data_parse(verizon_alerts)

def merge_verizon_data_with_schedule(dep,Schedule):
    # do not change the join columns setup
    join = pd.merge(dep,Schedule, how ='left', left_on = ['Alert Value_x','Alert Value_y'], right_on = ['OriginatingSite Code','TerminatingSite Code'])

    join_df = join[join['Transport ID'].notnull()]
    join_df = join_df.sort_values(by ='Vehicle' )
    join_df = join_df.reset_index(drop=True)
    return join_df
join_df = merge_verizon_data_with_schedule(dep,Schedule)

# create scheduling logic by setting a time boundary to show if the departure and arrival times are on schedule
def set_boundary_logic(join_df):
    ## Adding Minutes to make boundary

    early_time = -30
    late_time = 30

    join_df['Early_Dep_'] = join_df['OriginatingTime'] + timedelta(minutes = early_time)
    join_df['_Late_Dep'] = join_df['OriginatingTime'] + timedelta(minutes = late_time)

    join_df['Early_Arr_'] = join_df['TerminatingTime'] + timedelta(minutes = early_time)
    join_df['_Late_Arr'] = join_df['TerminatingTime'] + timedelta(minutes = late_time)

    early_on_time = -15
    late_on_time = 15

    join_df['Ontime_Dep_'] = join_df['OriginatingTime'] + timedelta(minutes = early_on_time)
    join_df['_Ontime_Dep'] = join_df['OriginatingTime'] + timedelta(minutes = late_on_time)

    join_df['Ontime_Arr_'] = join_df['TerminatingTime'] + timedelta(minutes = early_on_time)
    join_df['_Ontime_Arr'] = join_df['TerminatingTime'] + timedelta(minutes = late_on_time)

    boundary = join_df.drop(['Alert Value_x','Actual_Time_x','Actual_Time_y','Actual_Time_Diff','days','OriginatingSite Code','TerminatingSite Code','Duration'],axis=1)
    return boundary
boundary = set_boundary_logic(join_df)


def ETA(df):
    # Departure movement report
    df.loc[(df['Early_Dep_'] <= df['Departure_Time']) & (df['Departure_Time'] < df['Ontime_Dep_']) ,'Early_Departure'] = 'Early Dep'
    df.loc[(df['Ontime_Dep_'] < df['Departure_Time']) & (df['Departure_Time'] < df['_Ontime_Dep']) ,'On_Time_Dep'] = 'On Time Dep'
    df.loc[(df['_Ontime_Dep'] < df['Departure_Time']) & (df['Departure_Time'] <= df['_Late_Dep']) ,'Late_Departure'] = 'Late Dep'
    
    # Arrival movement report
    df.loc[(df['Early_Arr_'] <= df['Arrival_Time']) & (df['Arrival_Time'] < df['Ontime_Arr_']) ,'Early_Arrival'] = 'Early Arr'
    df.loc[(df['Ontime_Arr_'] < df['Arrival_Time']) & (df['Arrival_Time'] < df['_Ontime_Arr']) ,'On_Time_Arr'] = 'On Time Arr'
    df.loc[(df['_Ontime_Arr'] < df['Arrival_Time']) & (df['Arrival_Time'] <= df['_Late_Arr']),'Late_Arrival'] = 'Late Arr'

    travel_time = df['Scheduled_Duration']
    hours_time = df['hours']
    time_diff = abs(hours_time - travel_time)
    df['Actual vs Scheduled'] = time_diff.round(2)     
    travel_time = df['Scheduled_Duration']
    hours_time = df['hours']
    df.loc[(df['hours'] >= 2*travel_time) | (df['Scheduled_Duration'] >= 2*hours_time), 'Actual Duration'] = 'Out'
    df['Actual Duration'].fillna('Near Time', inplace=True)
    # determine if ETA is On Schedule
    df.loc[(df['Early_Departure'] == 'Early Dep') & (df['On_Time_Arr'] == 'On Time Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['Early_Departure'] == 'Early Dep') & (df['Late_Arrival'] == 'Late Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['Early_Departure'] == 'Early Dep') & (df['Early_Arrival'] == 'Early Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['Late_Departure'] == 'Late Dep') & (df['Late_Arrival'] == 'Late Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['Late_Departure'] == 'Late Dep') & (df['On_Time_Arr'] == 'On Time Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['Late_Departure'] == 'Late Dep') & (df['Early_Arrival'] == 'Early Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['On_Time_Dep'] == 'On Time Dep') & (df['On_Time_Arr'] == 'On Time Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['On_Time_Dep'] == 'On Time Dep') & (df['Early_Arrival'] == 'Early Arr'), 'On Schedule'] = 'On Schedule'
    df.loc[(df['On_Time_Dep'] == 'On Time Dep') & (df['Late_Arrival'] == 'Late Arr'), 'On Schedule'] = 'On Schedule'
    df['On Schedule'].fillna('Miss', inplace=True)
    return df
time = ETA(boundary)

def on_schedule_but_miss(df):
    df.loc[(df['On Schedule'] == 'Miss') & (df['Actual Duration'] == 'Near Time'), 'On Schedule'] = 'On Schedule/Miss'
    return df
ETA = on_schedule_but_miss(time)

Schedule_boundary = ETA[(ETA['Late_Departure'] == 'Late Dep') 
                             | (ETA['Early_Departure'] == 'Early Dep')  
                              | (ETA['Late_Arrival'] == 'Late Arr') 
                             | (ETA['Early_Arrival'] == 'Early Arr')
                             | (ETA['On_Time_Dep'] == 'On Time Dep' ) 
                             | (ETA['On_Time_Arr'] == 'On Time Arr' )
                            | (ETA['On Schedule'] == 'On Schedule')
                             
 ]

# merge track and trace schedule with tracked vehicles
NMR = pd.merge(Schedule,Schedule_boundary, how ='inner', on = 'Transport ID')


NMR = NMR[
        (NMR['Late_Departure'] == 'Late Dep')  
    | (NMR['Early_Departure'] == 'Early Dep')  
    | (NMR['Late_Arrival'] == 'Late Arr') 
    | (NMR['Early_Arrival'] == 'Early Arr')
    | (NMR['On_Time_Dep'] == 'On Time Dep' )
    | (NMR['On_Time_Arr'] == 'On Time Arr' )
    | (NMR['On Schedule'] == 'On Schedule')
                              
 ]

NMR = NMR.drop_duplicates(subset = ['Transport ID'])
NMR = NMR.fillna('--')
NMR = NMR.reset_index(drop=True)

NMR = pd.merge(Schedule,NMR, how='left', on = 'Transport ID')
NMR = NMR.fillna('Miss')
NMR.set_index('Transport ID', inplace=True)

NMR['Originating_Site'] = NMR['OriginatingSite Code_x']
NMR['Terminating_Site'] = NMR['TerminatingSite Code_x']
NMR['Duration'] = NMR['Duration_x'] 
NMR['Actual Travel Time'] = NMR['hours']


NMR = NMR.drop(['OriginatingTime_x','TerminatingTime_x',
                'Mon_x','Tue_x','Wed_x','Thu_x','Fri_x','Sat_x','Sun_x',
                'VehicleType_x','Scheduled_Duration_x','Alert Value_y','OriginatingTime_y','TerminatingTime_y',
                'Mon_y','Tue_y','Wed_y','Thu_y','Fri_y','Sat_y','Sun_y',
                'VehicleType_y','Scheduled_Duration_y','Actual Duration',
                'Early_Dep_','_Late_Dep','Early_Arr_','_Late_Arr','Ontime_Dep_','_Ontime_Dep','Ontime_Arr_','_Ontime_Arr',
                'Duration_y','OriginatingSite Code_y','TerminatingSite Code_y',
                'OriginatingSite Code_x','TerminatingSite Code_x','Duration_x','hours'],axis = 1)

# keep and rearrange dataframe features for network movement report
NMR = NMR[['VehicleType','Originating_Site','OriginatingTime','Departure_Time','Departure_Date',
           'Terminating_Site','TerminatingTime','Arrival_Time','Arrival_Date','Scheduled_Duration', 
           'Mon', 'Tue', 'Wed', 'Thu', 'Fri','Sat', 'Sun',
           'Early_Departure', 'On_Time_Dep', 'Late_Departure',
           'Early_Arrival', 'On_Time_Arr', 'Late_Arrival',
           'Actual Travel Time','Duration','Actual vs Scheduled',
           'On Schedule','Vehicle']]


NMR.to_csv('nmr_output_file.csv')
print(NMR)

