# -*- coding: utf-8 -*-
"""
Created on Thu Apr 7 12:01:54 2022
Update version on the 22/2/23
Update version on the 03/3/23
Update version on the 09/6/23
Update version on the 13/7/23
Update version on the 12/10/23
Update version on the 1/11/23
Update version on the 15/11/23

@author: madsend

Company: An Post

Depatment: Tranport (DMC)
"""
import pandas as pd
from datetime import datetime


today = datetime.today()
d = today.strftime("%Y:%m:%d")
day = pd.to_datetime(d, format='%Y:%m:%d')

## This method

def prepDataset(data):
   
    """ This function is to "Trim" & "Format" the data """
        
    data['Driver Name_1'] = data.loc[:,'Driver Name']
    data['Card Fleet No'] = data['Driver Name_1'].str.split(' ').str[0]
    
    data.loc[:,'Drawing Vehicle'] = data['Drawing Vehicle'].fillna('none').str.replace(" ", "")
    data.loc[:,'Drawing Vehicle'] = data['Drawing Vehicle'].str.strip()
    data.loc[:,'Card Vehicle'] = data['Card Vehicle'].str.strip()
    data.loc[:,'Driver Name'] = data['Driver Name'].str.strip()
    data.loc[:,'Date and Time'] = pd.to_datetime(data['Date and Time'].astype(str), format ='%Y-%m-%d %H:%M:%S')
    data = data.drop(['Driver Name_1'] , axis = 1)
    data.fillna('No Data', inplace=True)
    
    return data


def matchFleetDataWithDrawingVehicle(prov, fleet):
    
    fleet.loc[:,'REG_NUMBER'] = fleet['REG_NUMBER'].fillna('none').astype(str)
    
    # Get Card Vehicle match with Drawing Vehicles - Last 4 characters only
    drawingMatchCard = prov[prov.apply(lambda x: x['Drawing Vehicle'][-4:] == x['Card Vehicle'][-4:], axis=1)].reset_index(drop=True)
    
    # Replace Drawing Vehicle with Card Vehicle
    drawingMatchCard.loc[:,'Drawing Vehicle'] = drawingMatchCard['Card Vehicle']
    
    # join matched rows back to master and drop duplicates
    join1 = [drawingMatchCard, prov]
    join1 = pd.concat(join1)
    #print(join.columns)
    join1 = join1.drop_duplicates(['Site Name', 
                                   'Product Description', 'Date and Time', 'Volume', 'Unit Price',
                                   'Net Cost', 'Vat', 'Total', 'Card Vehicle',
                                   'Driver Name', 'Mileage'])
    

    # Left join Fleet Reg match from Tranman with Drawing Vehicles
    leftJoin1AndReg   = join1.merge(fleet, left_on = 'Drawing Vehicle', right_on = 'REG_NUMBER', how = 'left' )
    matchOnlyDrawAndReg = leftJoin1AndReg.loc[~(leftJoin1AndReg['FLEET_NUMBER'].isna())]
    
    # Get unmatched Fleet Reg with Drawing Vehicle by filtering Fleet Number
    nonMatchOnlyDrawAndReg = leftJoin1AndReg.loc[(leftJoin1AndReg['FLEET_NUMBER'].isna())]
    
    nonMatchOnlyDrawAndReg = nonMatchOnlyDrawAndReg.drop(['FLEET_NUMBER', 'REG_NUMBER', 
                                                          'DEPOT','Tonnage', 
                                                          'Part of Fleet'], axis=1)
    
   
    # Get Fleet No match with Drawing Card - Last 4 characters only
    drawingMatchFleetNo = nonMatchOnlyDrawAndReg[nonMatchOnlyDrawAndReg.apply(lambda x: x['Drawing Vehicle'][-4:] == x['Card Fleet No'][-4:], axis=1)].reset_index(drop=True)
    
    # Replace Drawing Vehicle with Card Vehicle
    drawingMatchFleetNo.loc[:,'Drawing Vehicle'] = drawingMatchFleetNo['Card Vehicle']
    
    # Left join Reg Number from Tranman with Drawing Vehicles from prov
    mergeDrawAndFleetReg   = drawingMatchFleetNo.merge(fleet, left_on = 'Drawing Vehicle', right_on = 'REG_NUMBER', how = 'left' )
    
    # join matched and non matched dataframes and drop duplicates 
    matchToNon = [matchOnlyDrawAndReg, mergeDrawAndFleetReg, nonMatchOnlyDrawAndReg]
    matchToNon = pd.concat(matchToNon)
    
    matchToNon = matchToNon.drop_duplicates(['Site Name',
                                             'Product Description', 'Date and Time', 'Volume', 'Unit Price',
                                             'Net Cost', 'Vat', 'Total', 'Card Vehicle',
                                             'Driver Name', 'Mileage'])
    
    
    # Left join Reg Number from Tranman with Drawing Vehicles from prov
    matchToNon = matchToNon.merge(fleet, left_on = 'Card Fleet No', right_on = 'FLEET_NUMBER', how = 'left' )
    
    matchToNon.loc[(matchToNon['FLEET_NUMBER_x'].isna()) ,'Part of Fleet_x'] = matchToNon['Part of Fleet_y']
    
    matchToNon.loc[(matchToNon['Volume'] < 90) & (matchToNon['Part of Fleet_x'].isna()) ,'Part of Fleet_x'] = 'Final Mile'
   
    matchToNon.loc[:,'Part of Fleet_x'].fillna('Middle Mile', inplace=True)
    
    matchToNon.loc[:,'Drawing Match FuelCard'] = None
    matchToNon.loc[:,'Drawing Reg Recorded'] = None
    
    matchToNon.reset_index(drop=True, inplace=True)
    
    return matchToNon


def matchVehicleDrawingToCardVehicle(matchToNon):
    
    regMatch = matchToNon[matchToNon.apply(lambda x: x['Drawing Vehicle'] == x['Card Vehicle'], axis=1)]
    regMatch.loc[:,'Drawing Match FuelCard'] = 'Yes'
    regMatch.loc[:,'Drawing Reg Recorded'] = 'Yes'
   
    regMatch.reset_index(drop=True, inplace=True)
    
    regMatch1 = [regMatch, matchToNon]
    regMatch2 = pd.concat(regMatch1)
    
    regMatch2 = regMatch2.drop_duplicates(['Site Name',
                                           'Product Description', 'Date and Time', 'Volume', 'Unit Price',
                                           'Net Cost', 'Vat', 'Total', 'Card Vehicle',
                                           'Driver Name', 'Mileage'])
    
    regMatch2.loc[:,'Drawing Match FuelCard'].fillna('No', inplace=True)
    
    return regMatch2
        

def isRegRecorded(regMatch2):
    
    regMatch2.loc[(regMatch2['Drawing Vehicle'].str.len() >= 4) 
                  & (regMatch2['Drawing Vehicle'].str.contains('[A-Za-z]') 
                     & (regMatch2['Drawing Vehicle'].str.contains('[0-9]') 
                        & (regMatch2['Drawing Reg Recorded'].isna()))) ,'Drawing Reg Recorded'] = 'Yes'
    
    regMatch2.loc[:,'Drawing Reg Recorded'].fillna('No', inplace=True)
    
    regMatch2.reset_index(drop=True, inplace=True)
    
    return regMatch2


def drawingVehicleMatchTranManRegOrFleetNo(regRecorded):
    
    regRecorded.loc[(regRecorded['FLEET_NUMBER_x'].isna()) ,
                  'Drawing_Match_TranManREG'] = 'No'
    
    regRecorded.loc[:,'Drawing_Match_TranManREG'].fillna('Yes', inplace=True)
    
    regRecorded.loc[((regRecorded['FLEET_NUMBER_y'].isna()) & (regRecorded['FLEET_NUMBER_y'].isna())),
                  'CardFleetNo_Match_TranManFleetNo'] = 'No'
    
    regRecorded.loc[:,'CardFleetNo_Match_TranManFleetNo'].fillna('Yes', inplace=True)
    
    regRecorded.reset_index(drop=True, inplace=True)
    
    return regRecorded
    

def lowFillCheck(matchToTranManReg):  
    
    matchToTranManReg.loc[(matchToTranManReg['Net Cost'] < 20) 
                                 & (matchToTranManReg['Product Description'] == 'Ire Derv') ,'Low Fill < 20'] = 'Yes'
    
    matchToTranManReg.loc[:,'Low Fill < 20'].fillna('No', inplace=True)
    
    matchToTranManReg['Fuel Company'] = "Maxol"
    
    matchToTranManReg.reset_index(drop=True, inplace=True)
    
    
    #print(matchToTranManReg.columns)
    
    matchToTranManReg = matchToTranManReg.drop(['FLEET_NUMBER_y', 'REG_NUMBER_y',
                                                'DEPOT_y', 'Tonnage_y', 'Part of Fleet_y','Mileage'], axis=1)
    
    
    
    matchToTranManReg.rename(columns = {'DEPOT_x':'DEPOT', 'Tonnage_x':'Tonnage', 'Part of Fleet_x':'Part of Fleet',
                                        'FLEET_NUMBER_x':'FLEET_NUMBER_MatchTranReg', 'REG_NUMBER_x':'REG_NUMBER_MatchTranReg'}, inplace = True)
    
   
    return matchToTranManReg


fleet_file = pd.read_csv(r'fleet_org_spec_data.csv')
fleet = pd.DataFrame(fleet_file)
fleet = fleet.drop(['Unnamed: 0', 'ODOMETER_DIS', 'ODOMETER_DAT','MODEL',
                    'MAKE', 'VEHICLE_TYPE', 'FUEL_USED', 'STATUS_CODE',
                    'FINANCE_METH', 'Avg ltr/100km', 'Avg Idling ltr/hr',
                     'Region', 'Cluster', 'Regional Manager',
                    'Operations Manager'], axis=1)
    
prov3 = pd.concat(pd.read_excel('provider3_master_file.xlsx', sheet_name=None), ignore_index=True)
prov3 = prepDataset(prov3)

prov3  = prov3.drop(['Cost Centre', 'Invoice No','Full Card Number','Status', 'Expense Code', 'GL Code','Cost Centre2',
                     'Drawing Id', 'Card Type', 'Card Number', 'ISO Number',
                     'Account Number', 'Short Card Number', 'Check Digit',
                     'Site Number', 'Product Code'], axis=1)
    
matchToNonmatch = matchFleetDataWithDrawingVehicle(prov3, fleet)

regMatch = matchVehicleDrawingToCardVehicle(matchToNonmatch)
   
regRecorded = isRegRecorded(regMatch)

drawMatchToTranMan = drawingVehicleMatchTranManRegOrFleetNo(regRecorded)
    
lowFillCheck = lowFillCheck(drawMatchToTranMan)

    
    
    

