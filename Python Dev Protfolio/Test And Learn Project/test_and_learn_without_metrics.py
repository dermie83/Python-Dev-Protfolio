# -*- coding: utf-8 -*-
"""
Created on Thu Oct 13 10:01:14 2022

@author: madsend

Company: An Post

Depatment: Tranport (DMC)
"""

import pandas as pd


def prepDetailedReport(df):
    
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    
    df["Date"] = df['Date Time'].dt.strftime('%Y/%m/%d')
    df["Date"] = pd.to_datetime(df['Date'], format='%Y/%m/%d')
    
    df["Time"] = df['Date Time'].dt.strftime('%H:%M:%S')
    df["Time"] = pd.to_datetime(df['Time'], format='%H:%M:%S')
    
    df['Weekday_name'] = df['Date'].dt.day_name()
    df['Week'] = df['Date'].dt.isocalendar().week
    
    df = df.drop(['Speed','Delta time (seconds)','Delta time','Driver Number','Driver Name','Employee ID', 'Postal code','Place IDs',
                  'Speed Limit', 'Heading', 'Time Zone', 'Time Zone Offset', 'ESN','Is Asset', 'Fuel Type', 
                  'Latitude', 'Longitude','Accumulated Time','Accumulated Distance' ], axis = 1)
    
    return df
    
    
def createVehicleActivityDataset(df, fleet, *depots):
     
    # get end of route at ignition off
    df_max = df.groupby('Vehicle Number', sort=False).apply(lambda x: x[x['Date Time'] == x['Date Time'].max()]).reset_index(drop=True)
    df_max = df_max[(df_max['Status'] ==  "Ignition Off")]
    df_max = df_max.drop_duplicates(subset = ['Vehicle Number',	'Vehicle Name',	'Registration Number',	'Street Address'])
    #df_max['Route Stage'] = ""
    df_max['Route Stage'] = "End Of Shift"
    # excelfilename = "fleet_garaging_max.csv"
    # df_max.to_csv(excelfilename)   
    
    for _ in depots:
        
        #df['End Of Shift'] = ""
        df = df[(df['Status'] !=  "Power Restored")]
        df = df[(df['Status'] !=  "Power Disruption")].reset_index(drop=True)
        
        df.loc[(df['Street Address'].astype(str).str.contains("O'Casey Ave")) ,'Place Names'] = 'Park West'
       
        df = df.loc[df.apply(lambda x: x['Place Names'] in _ , axis=1 )].reset_index(drop=True)
        
        df['Daily accumulated distance_1'] = df['Daily accumulated distance'].diff().gt(.05)
        df['Accumulated Time (Seconds)_1'] = df['Accumulated Time (Seconds)'].diff().gt(100)
        
        # excelfilename1 = "df_data.csv"
        # df.to_csv(excelfilename1)
        
        ########################## this is to check if the vehicle is parked at home or in depot ##########################################################################
        df_min = df.groupby('Vehicle Number', sort=False).apply(lambda x: x[x['Date Time'] == x['Date Time'].min()]).reset_index(drop=True)
        df_min = df_min.drop_duplicates(subset = ['Vehicle Number',	'Vehicle Name',	'Registration Number',	'Street Address'])
        
        
        df_min.loc[(df_min['Daily accumulated distance'] > 1), 'Route Stage'] = 'Home Garaging'
        df_min.loc[(df_min['Daily accumulated distance'] < 1), 'Route Stage'] = 'Depot Garaging'
        ##############################################################################################################################################
        
        # df refiltered to get start and end of route
        df.loc[(df['Daily accumulated distance_1'] == True) & 
                        (df['Accumulated Time (Seconds)_1'] == True) ,'Route Stage'] = 'End Of Route'
        
        
        df['Start of Route'] = df['Route Stage'].shift(-1)
        df['Start of Route'] = df['Start of Route'].replace('End Of Route', 'Start Of Route', regex=True)
                  
        
        df.loc[(df['Start of Route'] == 'Start Of Route') ,'Route Stage'] = 'Start Of Route'
        
        df = df.drop(['Start of Route'], axis=1)
        
        df = df.drop(df[(df['Daily accumulated distance_1'] != True) & (df['Accumulated Time (Seconds)_1'] != True) &
                        (df['Route Stage'] != 'Start Of Route' )].index)
        
        df = df.drop(df[(df['Daily accumulated distance_1'] == False) & (df['Accumulated Time (Seconds)_1'] == True) &
                        (df['Route Stage'] != 'Start Of Route' )].index)
        
       
        df = df.loc[(df['Route Stage'].str.len() > 0)]
        
        # excelfilename = "start_stop.csv"
        # df.to_csv(excelfilename)   
    
        dataframes = [df_min, df, df_max]
        df_1 = pd.concat(dataframes).reset_index(drop=True)
        excelfilename = "merge_1.csv"
        df_1.to_csv(excelfilename)   
    
        df_1['sequence'] = df_1.sort_values(by=['Vehicle Number', 'Date Time']).groupby(['Vehicle Number']).cumcount() + 1
        
        
        df_1 = df_1.drop(['Vehicle Name', 'Registration Number', 'Status', 'Street Address', 
                                      'City', 'County', 'Odometer',
                                      'Ignition', 'Date', 'Time', 
                                      'Weekday_name', 'Week', 'Delta distance',
                                      'Daily accumulated distance_1','Accumulated Time (Seconds)_1' ], axis=1)
        
        dfs = [x for _, x in df_1.groupby('sequence')]
       
        data_merge = pd.concat([x.set_index('Vehicle Number') for x in dfs],axis = 1,keys=range(len(dfs)))
        data_merge.columns = data_merge.columns.map('{0[1]}_{0[0]}'.format)
        # data_merge = data_merge.reset_index()
       
        fleet_1 = fleet.drop(['ACTIVITY','ODOMETER_DAT','ODOMETER_DIS','ADMINISTOR','DEMANDACTIVITY'], axis=1)	

        merge_fleet = fleet_1.merge(data_merge, left_on = ['FLEET_NUMBER'] , right_on = ['Vehicle Number'], how = 'left' )
        
        merge_fleet.loc[(merge_fleet['VEHICLE_TYPE'] == 'Truck') | (merge_fleet['VEHICLE_TYPE'] == 'Trailer') & 
                        (merge_fleet['Route Stage_0'].str.len() > 0) , 'Route Stage_0'] = 'Depot Garaging'
        
        
        excelfilename = "route_stage_output_file.csv"
        merge_fleet.to_csv(excelfilename)
        
    return merge_fleet


if __name__ == '__main__':
    
    
    depots = ['DSO-Lettermore', 'DSO-Knocknagree', 'DSO-Ballydehob', 'Harris Retail', 'DSO-Enniskeane', 'Red Abbey Motors', 'DSU-Portlaoise', 
              'Blackstone Motors Dundalk', 'DSU-Newbridge', 'Truckcar Sales', 'DSO-Castleisland', 'HARRIS Group', 'DSO-Carraroe', 'Kevin Culkin Commercials',
              'DSO-Ballymahon', 'DSO-Rathdowney', 'Western Motors Drogheda', 'M&J Commercials Tralee', 'DSU-Letterkenny', 'Deerpark Motors 1', 'Greenogue Customs Depot', 
              'DSU-Ballina', 'DSO-Ballinamore', 'DSO-Mohill', 'DSU-Cavan', 'C.A.B. Motor Co. Ltd', 'DSO-Milltown', 'Dineens Crash Repairs', 'McElvaney Motors Monaghan',
              'DSU-Boyle', 'PK Motors, Donegal', 'Wexford Volkswagen', 'DSO-Boherbue', 'DSO-Recess', 'Tullow HGV Service', 'DSU-Cork North City', 'Ace Autobody Clonmel',
              'Lahart Garages Ltd', 'DSO-Dunmanway', 'Cassidys Garage, Knockmay', 'DSO-Charlestown', 'DSO-Dungloe', 'DSO-Portarlington', 'DSO-Ballybofey', 
              'DSU-Dublin 8', 'Dan Ryan Repairs', 'DSU-Ennis', 'DSU-Balbriggan', 'Blackwater Motors Cork', 'Portlaoise Mail Centre', 'Mullingar Opel/Nissan', 
              'DSU-Bantry', 'DSO-Timoleague', 'DSO-Oughterard', 'Lifford Coachworks', 'Mutec', 'Ace Autobody, Limerick', 'Technical Support Services (Dublin)', 
              'DSO-Menlough', 'Ace Autobody Naas', 'Surehaul', 'M&R Motors', 'DSU-Wexford', 'DSU-Ballsbridge', 'M7-Rest Stop TP', 'DSO-Buncrana', 'DSU-Lucan', 
              'DSO-Killucan', "Hogan's Garage, Ennis", 'DSU-Waterford', "Lyon's of Limerick", 'DSO-Schull', 'DSO-Glengarriff', 'M&R Autos', 'DSU-Charleville', 
              'DSO-Ballybay', 'DSO-Ardee', 'Windsor Galway', 'Brian Keogh Commercials, Kilkenny', 'DSU-Dublin 14', 'DSU-Sligo', 'DSO-Inniscrone', 
              'Dublin Mail Centre', 'DSU-Birr', 'Cawley Commercials, Sligo', 'DSO-Mountrath', 'DSU-Wicklow', 'Blackstone Motors', 'DSO-Brosna',
              'DSU-Drogheda', 'Connolly Car Sales Sligo Ltd. VW', 'DSO-Gortahork', 'DSO-Geesala', 'DSU-Dublin 12', 'Kearys Nissan', 'DSU-Ballinasloe', 
              'DSU-Kells', 'Oriel Auto Specialists', 'Murphy Motor Services Kerry', 'DSU-Dublin 9', 'DSU-Fermoy', 'DSO-Gurteen', 'Donagh Hickey',
              'DSU-Dublin 10', 'DSO-Inis Oirr', 'Reens Garage', 'DSO-Pallasgreen', 'Technical Support Services (Limerick)', "KRC Motors/Carr's Garage Drogheda", 
              'DSO-Castlegregory', 'DSO-Murroe', 'DSO-Kilronan', 'Jay Commercials, Cork', 'DSU-Youghal', 'Joe Norris Motors', 'DSU-Claremorris', 'Bogue Motors Ltd.', 
              'DSO-Killorglin', 'PK Motors, Blackrock', 'DSU-Lifford', 'DSO-Belmullet', 'DSO-Kilkieran', 'DSO-Drinagh', 'DSO-Falcarragh', 
              'Chambers Refrigeration, Letterkenny', 'Ace Autobody, Wexford', 'Ted McSweeney Refrigeration, Cobh', 'DAF Truck Services, Mallow', 
              'Technical Support Services Roscommon', "Brady's Arva Ltd", 'Logic Fleet Management', 'DSU-Maynooth', 'DSO-Lislevane', 'Interparts, Cavan',
              'DSO-Portumna', 'DSU-Glenageary', 'Western Motors Ltd (Galway)', 'DSO-Lisdoonvarna', 'DSU-Tuam', 'DSO-Dingle', 'DSU-Carlow', 'Belgard Renault',
              "Dennehy's Limerick", 'DSO-Glenamoy', 'Ace Autobody Longmile Road', 'Surehaul Tipperary', 'DSU-Mitchelstown', 'DSU-Newcastle West', 'DSU-Navan', 
              'Broomhill', 'DSO-Scarriff', 'DSO-Kinvara', 'DSO-Gort', 'DSU-Carrickmacross', 'Nolan Motors (Longford)', 'DSU-Gorey', 'DSO-Askeaton', 
              'John McCabe Nissan', 'Ted Brennan Motors, Castleblayney', 'Colton Ford', 'DSU-Athy', 'DSO-Killybegs', 'Divanes Volkswagen Kerry', 'DSU-Tullamore',
              'DSO-Tallow', 'DSO-Rockchapel', 'DSU-Dublin 7', 'Holden Plant Kilkenny', 'PK Motors, Wicklow', 'DSU-Ballyhaunis', 'DSO-Dunkineely', 
              'Ace Autobody, Sligo', 'DSO-Ardara', 'DSU-Dungarvan', 'DSO-Kilfenora', 'DSU-Kinsale', 'Western Garages Ltd (Ennis)', 'DSO-Lauragh', 'Bandon Motors', 
              'McElvaney Motors Dublin', 'D&M Truck Engineering', "O'Reilly Commercials Ballinalack, Mullingar", 'Tom Murphy Car Sales Ltd (Service)', 
              'Autotech Bodyshop 2', 'Kilkenny Truck Centre', 'DSU-Dublin 11', 'DSU-Bandon', 'Windsor Deansgrange', 'Tullamore Motors', 'Fred Kilmartin Ltd', 
              'DSU-Belturbet', 'DSO-Carna', 'DSO-Castletownbere', 'DSO-Moylough', 'O Briens Kilkenny', 'DSO-Bealadangan', 'M7-Rest Stop LS', 'DSO-Barnatra',
              'S White/Athlone Renault', 'Newtown Commercials, Donegal', 'DSO-Gneeveguilla', 'DSU-Castleblayney', 'DSU-Dublin 16', 'DSU-Dundalk', 'DSO-Miltown Malbay', 
              'DSU-Clonakilty', 'DSO-Cappoquin', 'Windsor Airside Nissan', 'DSO-Dowra', 'H&H Motors', 'DSU-Midleton', 'DSU-Castlerea', 'DSO-Allihies', 'DSU-Dublin 24', 
              'DSU-Cashel', 'DSO-Rosmuck', 'DSU-Kilmallock', 'S&R Motors (Donegal) Ltd', 'DSO-Inishbofin', 'DSO-Kiltimagh', 'DSO-Ballynacargy', 'DSU-Tipperary', 
              'DSU-Kilrush', 'DSO-Kenmare', "Cavanagh's of Fermoy", 'K.D. Garage', 'DSU-Limerick', 'DSU-Tralee', 'DSO-Derrybeg', 'DSU-Killarney', 'HGV Switch Point', 
              'DSU-Mullingar', 'Autoimage', 'DSO-Kildysart', 'DSO-Ballyheigue', 'DSU-Blackrock', 'DSO-Lahinch', 'DSU-Mallow', 'DSU-Kilgarvan', 'DSO-Dromore West', 
              'DSO-Arranmore Island', 'DSO-Kilmacthomas', 'DSU-Nenagh', 'Kearys Renault Pro+ Service & Parts', 'Menapia Motors', 'DSU-Arklow', 'DSO-Conna', 'DSU-Listowel',
              'Blackwater Fermoy', 'Donegal Commercials', 'Highland Motors', 'Ace Autobody (Ballysimon)', 'DSO-Ardfert', 'Trinity Volkswagen', 'DSO-Eyeries', 
              'DSO-Ennistymon', 'DSO-Easkey', 'DSO-Mulrany', 'DSO-Headford', 'Mullingar Autos (VW & Skoda Cars)', 'DSO-Doocastle', 'DSU-Holly Rd', 'DSO-Bonniconlon', 
              'Ace Autobody, Letterkenny', 'Hurley Bros Bantry', 'DSU-Westport', 'DSU-Skibbereen', 'Auto Boland Waterford', 'DSO-Millstreet', 'DSU-Dublin 17', 
              'DSO-Newmarket', 'Keane J. & Sons (Ros) Ltd', 'DSO-Ardrahan', 'DSU-Dublin 1', 'DSO-Goleen', 'Sheehy Motors Carlow', 'DSU-Cork South City', 'DSU-Macroom', 
              'DSU-Dublin 15', 'DSU-Carrick-On-Shannon', 'DSU-Athenry', 'DSU-Shannon', 'DSU-Cobh', 'Joe Duffy Volkswagen (Navan)', 'DSU-Boherbue', 'DSO-Kiskeam', 
              'DSO-Streamstown', 'DSO-Costelloe', 'Ace Autobody Finglas', 'DSO-Drimoleague', 'DSU-Dublin 22/Fonthill', 'DSU-Kilkenny', 'DSO-Ballyduff', 
              'DSO-Abbeyleix', 'DSO-Abbeyfeale', 'Hyundai (Navan)', 'DSO-Leenane', 'Autocraft, Wexford', 'DSU-New Ross', 'DSO-Lismore', 'K&M Heffernan', 
              'Sheehy Motors Naas', 'Blackstone Motors Cavan', 'DSO-Ballycroy', 'DSO-Durrus', 'DSU-Monaghan', 'Sleators Mullingar', 'Shaw Commericals, Castlebar',
              'Windsor Nissan Liffey Valley', 'DSO-Manorhamilton', 'DSO-Moville', 'DSU-Clones', 'DSO-Lissycasey', 'DSO-Foxford', 'Cleary Motors', 'DSU-Carrigaline', 
              'DSO-Tramore', 'DSO-Clonaslee', 'Donal Ryans Nenagh', 'Kellihers Garage', 'DSO-Bundoran', 'DSO-Bangor Erris', 'DSO-Ballineen', 'DSU-Dublin 18',
              'DSO-Lahardane', 'Boland Motors New Ross', 'DSO-Shrule', 'DSO-Kealkill', 'Athlone Mail Centre', 'DSO-Crossmolina', 'Don Butler Commercials',
              'DSO-Shantonagh', 'DSO-Watergrasshill', 'DSU-Enfield', 'DSO-Tulla', 'DSU-Dublin 6w', 'Hogan Frank Ltd.', 'Kilcoole Parcel Centre', 'DSO-Granard',
              "O'Briens Kilkenny", "Cassidy's Back Yard, Portlaoise", 'DSU-Clifden', 'Kingdom Crash Repairs', 'DSO-Knocknagoshel', 'DSO-Lettermullen', 
              'DSU-Dunshaughlin', 'DSO-Killaloe', 'Gethings Garage, Enniscorthy', 'DSO-Mountmellick', 'Ace Autobody, Portlaoise', 'DSU-Bray Extra', 
              'DMC and DPH', 'DSO-Doon', 'DSU-Little Island', 'DSU-Dublin 5', 'DSO-Sneem House', 'DSO-Moyard', 'Ace Autobody Bray', "Horan's Garage, Togher", 
              'DSU-Dublin 13', 'DSO-Kilbeggan', 'Caraher & Ward Ltd. (Dundalk)', 'DSU-Cahir', 'DSO-Carrowmore-Lacken', 'DSU-Dublin 3', 'DSO-Ballydesmond', 
              'Smiths of Drogheda', 'DSU-Roscommon', 'Doolan Commercial Repair Services , Athlone', 'ZEPRO UK (St. Helens)', 'DSU-Naas', 'DSU-Ballinrobe', 
              'DSO-Achill Sound', 'Pierse Motors Ltd Tipperary', 'DSU-Athlone', 'DSO-Ballyvaughan', 'DSO-Muff', 'Ace Autobody Galway', 'Kearys Renault', 
              'DSU-Clonmel', 'DSO-Clare Island', 'DSU-Longford', 'P&H Doyles (Dacia Wexford)', 'Dore Commercials, Limerick', 'Autotech Bodyshop Cavan', 
              'DSO-Newcastle West', 'Joe Mallon Motors Portlaoise', 'Accident Repair Centre', 'DSU-Castlebar', "O'Brien's Mullingar", 'Ace Autobody Coolock', 
              'DSO-Carrick', 'DSO-Dunfanaghy', 'DSO-Castlepollard', 'Newmarket Motor Works', 'DSU-Ballincollig', 'DSO-Ballyshannon', "Cavanagh's of Charleville 1", 
              'Bright Volkswagen Commercial Vehicles', 'Ace Autobody Ballina', 'Greenhall Motors Ltd Main Opel Dealer', 'DSU-Tullow', 'Joe Mallon Motors, Naas', 
              'Porter Ford Sligo', 'DSO-Arva', 'Cavan Autoparc', 'Johnson & Perrot Douglas', 'DSO-Inis Meain', 'DSU-Galway', 'Ace Autobody Athlone', 
              'Technical Support Services Antrim', 'DSO-Ballynahown', 'DSU-Caherciveen', 'DSU-Thurles', 'DSU-Greystones', 'DSU-Cootehill', 'DSO-Adrigole', 
              'DSU-Carrick-On-Suir', 'DSU-Donegal', 'DSO-Carrigallen', 'DSU-Loughrea', 'DSO-Feakle', 'DSU-Bray', 'DSU-Dublin 6', 'Conlon & Son Forklift & Commercials, Sligo',
              'DSO-Abbeydorney', 'AL Hayes Motors Ltd', 'Cunningham Autobody, Offaly', 'DSU-Enniscorthy', 'Sleator Motors Kia Mullingar', 'DSO-Rathowen', 
              'Dublin Tunnel Commercials', 'DSO-Tubbercurry', 'Park West', 'Surehaul Waterford', "Walsh's Garage", 'DSO-Glencar', 'DAF Trucks, Baldonnell', 
              'M.E.R.A.', 'GPO', 'Test Garage', 'Baldonnell Business Park', 'JJ Burkes Ballinrobe', 'Jack Sleator Motors', 'Blackwater Motors Skibbereen', 
              'DSO-Stradbally', 'DSO-Rosscarbery', 'Galway Truck and Van centre', 'Kevin Connolly Car Sales Ltd', 'DSO-Ballycastle', 'DSO-Glenties',
              'DSU-Dublin 6W', 'Garth Mckenna Refrigeration', 'DSO-Roundstone', 'DSU-Ballymote', 'DSO-Collinstown', 'DSU-Roscrea', 'DSO-Swinford', 
              'Dublin Parcel Hub', 'Monaghan & Sons Ltd', 'Kerry Truck Services & Coleman Repair Services', 'DSO-Carrowteige', 'DSO-Glendalough', 
              'DSU-Swords-Malahide', 'DSO-Killala', 'MerryWell', 'Ace Autobody Fairview', 'DSO-Carndonagh', 'DSU-Dublin 2/4', 
              'Drumconrath', 'Ardfinnan', 'Bruckless', 'Ballinalack', "Labasheeda", "DSU-Bray Extra, Beechwood Close", "H&H Motors, Waterford ZERO Emissions Zone",
              "DSU-Dublin 8, Dublin ZERO Emissions Zone", "DSU-Waterford, Waterford ZERO Emissions Zone", 
              "DSU-Dublin 7, Dublin ZERO Emissions Zone", "DSU-Kilkenny, Kilkenny ZERO Emissions Zone", 'Corrandulla', "DMC and DPH, Dublin Mail Centre",
              "Dublin Parcel Hub, Dublin Mail Centre"]
    
    
    sheet = ['FLEET']
    fleet = pd.read_excel(r'daily_snapshot_fleet.xlsx', sheet_name = sheet[0])
    fleet_df = pd.DataFrame(fleet)
    
    
    # final_mile_detail_report = pd.read_excel(r'C:/Users/madsend/Desktop/Test and Learn/test for detail report.xlsx')
    # final_mile_detail_report_df = pd.DataFrame(final_mile_detail_report)
    
    
    final_mile_detail_report = pd.concat(pd.read_excel(r'test for detail report_290623.xlsx', sheet_name=None), ignore_index=True)
    
    final_mile_detail_report_df = pd.DataFrame(final_mile_detail_report)
    
    file_setup = prepDetailedReport(final_mile_detail_report_df)
    
    start_stop_setup = createVehicleActivityDataset(file_setup, fleet_df, depots)
    
    

    
    
    
    
    
