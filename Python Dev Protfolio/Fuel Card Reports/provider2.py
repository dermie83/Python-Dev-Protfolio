# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 12:01:54 2022
Update version on the 13/7/23
Update version on the 12/10/23
Update version on the 15/11/23

@author: madsend

Company: An Post

Depatment: Tranport (DMC)
"""
import pandas as pd


def prepDataset(data):
    
    """ This function is to "Trim" & "Format" the data to match headers with the other Fuel report headers"""
        
    data.loc[:,'Drawing Vehicle'] = data['Registration'].str.strip()
    data.loc[:,'Product Description'] = data['Product'].str.strip()
    data.loc[:,'Date and Time'] = pd.to_datetime(data['Tran date'].astype(str), format ='%Y-%m-%d %H:%M:%S')
    

    data.rename(columns = {'Quantity':'Volume', 'Retail Net':'Net Cost', 'Retail Gross':'Total',
                           'Code':'Expense Code', 'Cost centre':'Cost Centre', 'Site name':'Site Name',
                            'Vat amount':'Vat' }, inplace = True)
    
    data.fillna('Missing Data', inplace=True)
    
    data = data.drop(['Registration','Tran date','Product'], axis = 1)
    
    return data


def matchTranmanFleetDataWithFuelData(prov, fleet):
    
    fleet.loc[:,'REG_NUMBER'] = fleet['REG_NUMBER'].fillna('none').astype(str)
    
    addPartOfFleet = prov.merge(fleet, left_on = 'Drawing Vehicle', right_on = 'REG_NUMBER', how = 'left' )
    #addPartOfFleet =  addPartOfFleet.drop(['FLEET_NUMBER', 'REG_NUMBER','DEPOT', 'Tonnage'], axis=1)
    
    partOfFleet = addPartOfFleet.loc[~(addPartOfFleet['Part of Fleet'].isna())]
    
    noPartOfFleet = addPartOfFleet.loc[(addPartOfFleet['Part of Fleet'].isna())]
    noPartOfFleet = noPartOfFleet.drop(['Part of Fleet'], axis=1)
    
    mergeNoPartOfFleet = noPartOfFleet.merge(fleet, left_on = 'Drawing Vehicle', right_on = 'FLEET_NUMBER', how = 'left' )
    #mergeNoPartOfFleet =  mergeNoPartOfFleet.drop(['FLEET_NUMBER', 'REG_NUMBER','DEPOT', 'Tonnage'], axis=1)
    
    mergePartOfFleet = [partOfFleet,  mergeNoPartOfFleet]
    partOfFleetDF = pd.concat(mergePartOfFleet)
    
    partOfFleetDF = partOfFleetDF.drop(['FLEET_NUMBER_x', 'REG_NUMBER_x','DEPOT_x', 'Tonnage_x'], axis=1)
    
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
    
   
    regRecordedMatchTran = regRecorded
    
    return regRecordedMatchTran


def getLowFillAndHeaders(regRecordedMatchTran): 
    
    regRecordedMatchTran.loc[((regRecordedMatchTran['Net Cost'] < 20) & 
                             (regRecordedMatchTran['Product Description'] == 'DIESEL')) ,'Low Fill < 20'] = 'Yes'
    regRecordedMatchTran.loc[:,'Low Fill < 20'].fillna('No', inplace=True)
    
    regRecordedMatchTran.reset_index(drop=True, inplace=True)
    
    regRecordedMatchTran['Unit Price'] = 'No Data'
    regRecordedMatchTran['Card Vehicle'] = 'No Data'
    regRecordedMatchTran['Driver Name'] = 'No Data'
    regRecordedMatchTran['Card Fleet No']= 'No Data'
    regRecordedMatchTran['Fuel Company'] = "prov2"
    regRecordedMatchTran['Drawing Match FuelCard'] = 'No Data'
    regRecordedMatchTran.fillna('No Match', inplace=True)
    
    regRecordedMatchTran = regRecordedMatchTran.drop(['FLEET_NUMBER_y', 'REG_NUMBER_y',
                                                      'DEPOT_y', 'Tonnage_y'], axis=1)
    
    
    regRecordedMatchTran.rename(columns = {'FLEET_NUMBER':'FLEET_NUMBER_MatchTranReg', 'REG_NUMBER':'REG_NUMBER_MatchTranReg'}, inplace = True)
    
    
    lowFillAndHeaders = regRecordedMatchTran
    
    return lowFillAndHeaders


fleet_file = pd.read_csv(r'fleet_org_spec_data.csv')
fleet = pd.DataFrame(fleet_file)
fleet = fleet.drop(['Unnamed: 0','MODEL','MAKE', 'VEHICLE_TYPE', 'FUEL_USED',
                    'STATUS_CODE', 'ODOMETER_DIS', 'ODOMETER_DAT','FINANCE_METH', 
                    'Avg ltr/100km', 'Avg Idling ltr/hr', 'Region', 'Cluster', 
                    'Regional Manager', 'Operations Manager'], axis=1)

prov2 = pd.concat(pd.read_excel('provider2_master_file.xlsx', sheet_name=None), ignore_index=True)
prov2 = pd.DataFrame(prov2)

prov2 = prov2.drop(['batch', 'Customer No', 'ISO no', 'Card No',
                                'Cost centre', 'Status', 'Code', 'GL Code', 'Mileage'], axis=1)
print(prov2.columns)
    
prov2 = prepDataset(prov2)

merge1 = matchTranmanFleetDataWithFuelData(prov2, fleet)
    
isDrawingRegRecorded = isDrawingRegRecorded(merge1)

drawingMatchTranManRegOrFleetNo = drawingVehicleMatchTranManRegOrFleetNo(isDrawingRegRecorded)

lowFillAndHeaders = getLowFillAndHeaders(drawingMatchTranManRegOrFleetNo)




    

    
    

