# -*- coding: utf-8 -*-
"""
Created on Thu Apr 7 12:01:54 2022
Update version on the 12/10/23
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


def prepFuelDataset(data):
    
    """ This function is to "Trim" & "Format" the data to match headers with the Datasheet headers"""
    data.loc[:,'Registration'].fillna('Missing Data', inplace=True)
    data.loc[:,'Drawing Vehicle'] = data['Registration'].str.strip()
    data.loc[:,'Driver Name'] = data['Driver'].str.strip()
    
    data['Driver Name_1'] = data.loc[:,'Driver Name']
    data['Card Fleet No'] = data['Driver Name_1'].str.split(' ').str[0]
    
    data.loc[:,'Product Description'] = data['Product'].str.strip()
    data.loc[:,'Date and Time'] = pd.to_datetime(data['Tranasction Date'].astype(str), format ='%Y-%m-%d %H:%M:%S')
    

    data.loc[:,'Invoice CPL'].fillna(1, inplace=True)
    data['Invoice CPL'] = (data['Invoice CPL']/100)
    
    data.rename(columns = {'Quantity':'Volume', 'Net Vat':'Net Cost', 'Vat Value':'Vat', 'Gross Value':'Total',
                           'Site':'Site Name',
                           'Invoice CPL':'Unit Price'}, inplace = True)
    
    data = data.drop(['Registration', 'Driver',  'Product', 'Tranasction Date', 'Driver Name_1'], axis =1)
    
    data.fillna('No Data', inplace=True)
    data.reset_index(drop=True, inplace=True)
    
   
    return data


def matchTranmanFleetDataWithFuelData(prov, fleet):
    
    fleet.loc[:,'REG_NUMBER'] = fleet['REG_NUMBER'].fillna('none').astype(str)
    
    addPartOfFleet = prov.merge(fleet, left_on = 'Drawing Vehicle', right_on = 'REG_NUMBER', how = 'left' )
    #addPartOfFleet =  addPartOfFleet.drop(['FLEET_NUMBER', 'REG_NUMBER','DEPOT', 'Tonnage'], axis=1)
    
    partOfFleet = addPartOfFleet.loc[~(addPartOfFleet['Part of Fleet'].isna())]
    
    noPartOfFleet = addPartOfFleet.loc[(addPartOfFleet['Part of Fleet'].isna())]
    noPartOfFleet = noPartOfFleet.drop(['Part of Fleet'], axis=1)
    
    mergeNoPartOfFleet = noPartOfFleet.merge(fleet, left_on = 'Card Fleet No', right_on = 'FLEET_NUMBER', how = 'left' )
    #mergeNoPartOfFleet =  mergeNoPartOfFleet.drop(['FLEET_NUMBER', 'REG_NUMBER','DEPOT', 'Tonnage'], axis=1)
    
    mergePartOfFleet = [partOfFleet,  mergeNoPartOfFleet]
    partOfFleetDF = pd.concat(mergePartOfFleet)
    
    partOfFleetDF =   partOfFleetDF.drop(['FLEET_NUMBER_x', 'REG_NUMBER_x','DEPOT_x', 'Tonnage_x'], axis=1)
    
    partOfFleetDF.loc[(partOfFleetDF['Volume'] < 90) & (partOfFleetDF['Part of Fleet'].isna()) ,'Part of Fleet'] = 'Final Mile'
    partOfFleetDF.loc[:,'Part of Fleet'].fillna('Middle Mile', inplace=True)
    
    partOfFleetDF.loc[:,'Drawing Reg Recorded'] = None
    
    
    return  partOfFleetDF

       

def isDrawingRegRecorded(partOfFleetDF):
    
    partOfFleetDF.loc[(partOfFleetDF['Drawing Vehicle'].str.len() >= 4) 
                  & (partOfFleetDF['Drawing Vehicle'].str.contains('[A-Za-z]')
                     & (partOfFleetDF['Drawing Vehicle'].str.contains('[0-9]') 
                        & (partOfFleetDF['Drawing Reg Recorded'].isna()))) ,'Drawing Reg Recorded'] = 'Yes'
    
    partOfFleetDF.loc[:,'Drawing Reg Recorded'].fillna('No', inplace=True)
    
    partOfFleetDF.reset_index(drop=True, inplace=True)
    
    
    regRecorded = partOfFleetDF

    return regRecorded


def drawingVehicleMatchTranManRegOrFleetNo(regRecorded):
    
    regRecorded.loc[(regRecorded['FLEET_NUMBER'].isna()) ,
                  'Drawing_Match_TranManREG'] = 'No'
    
    regRecorded.loc[:,'Drawing_Match_TranManREG'].fillna('Yes', inplace=True)
    
    regRecorded.loc[((regRecorded['FLEET_NUMBER'].isna()) & (regRecorded['FLEET_NUMBER_y'].isna())) ,
                  'CardFleetNo_Match_TranManFleetNo'] = 'No'
    
    regRecorded.loc[:,'CardFleetNo_Match_TranManFleetNo'].fillna('Yes', inplace=True)
    
    regRecorded.reset_index(drop=True, inplace=True)
    
    return regRecorded


def getLowFillAndHeaders(regRecordedMatchTran): 
    
    regRecordedMatchTran.loc[((regRecordedMatchTran['Net Cost'] < 20) & ((regRecordedMatchTran['Product Description'] == 'MILES DIESEL') |
                             (regRecordedMatchTran['Product Description'] == 'MILESPLUS DIESEL'))) ,'Low Fill < 20'] = 'Yes'
    regRecordedMatchTran.loc[:,'Low Fill < 20'].fillna('No', inplace=True)
    
    regRecordedMatchTran.reset_index(drop=True, inplace=True)
    
    #regRecordedMatchTran.loc[:,'DEPOT'].fillna('No Match', inplace=True)
    
    regRecordedMatchTran['Card Vehicle'] = 'No Data'
    #regRecordedMatchTran['Driver Name'] = 'No Data'
    regRecordedMatchTran['Card Fleet No']= 'No Data'
    regRecordedMatchTran['Fuel Company'] = "prov1"
    regRecordedMatchTran['Drawing Match FuelCard'] = 'No Data'
    regRecordedMatchTran.fillna('No Match', inplace=True)
    
    
    regRecordedMatchTran = regRecordedMatchTran.drop(['FLEET_NUMBER_y', 'REG_NUMBER_y',
                                                      'DEPOT_y', 'Tonnage_y'], axis=1)
    
    
    
    regRecordedMatchTran.rename(columns = {'FLEET_NUMBER':'FLEET_NUMBER_MatchTranReg', 'REG_NUMBER':'REG_NUMBER_MatchTranReg'}, inplace = True)
    
    excelfilename = 'prov1Checks.csv'
    regRecordedMatchTran.to_csv(excelfilename) 
    
    lowFillAndHeaders = regRecordedMatchTran
    
    return lowFillAndHeaders


fleet_file = pd.read_csv(r'fleet_org_spec_data.csv')
fleet = pd.DataFrame(fleet_file)
fleet = fleet.drop(['Unnamed: 0','MODEL','MAKE', 'VEHICLE_TYPE', 'FUEL_USED',
                    'STATUS_CODE', 'ODOMETER_DIS', 'ODOMETER_DAT','FINANCE_METH', 
                    'Avg ltr/100km', 'Avg Idling ltr/hr', 'Region', 'Cluster', 
                    'Regional Manager', 'Operations Manager'], axis=1)
                    


prov1 = pd.concat(pd.read_excel('provider1_master_file.xlsx', sheet_name=None), ignore_index=True)
prov1 = pd.DataFrame(prov1)
prov1 = prov1.drop(['Pan Number', 'Cost centre', 'Status', 'Code', 'GL Code', 'Mileage'], axis=1)
prov1 = prepFuelDataset(prov1)
    
merge1 = matchTranmanFleetDataWithFuelData(prov1, fleet)
    
isDrawingRegRecorded = isDrawingRegRecorded(merge1)

drawingMatchTranManRegOrFleetNo = drawingVehicleMatchTranManRegOrFleetNo(isDrawingRegRecorded)

lowFillAndHeaders = getLowFillAndHeaders(drawingMatchTranManRegOrFleetNo)

    
    

