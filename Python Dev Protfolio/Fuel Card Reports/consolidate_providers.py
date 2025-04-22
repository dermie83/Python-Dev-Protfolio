# -*- coding: utf-8 -*-
"""
Created on Mon Mar  6 15:53:42 2023

@author: madsend
"""

import pandas as pd

from provider1 import lowFillAndHeaders as prov1
from provider2 import lowFillCheck as prov2
from provider3 import lowFillAndHeaders as prov3


def joinFuelCardCheckData(prov1, prov2, prov3):  
    fuel_card_checks = [prov1, prov2, prov3]
    df = pd.concat(fuel_card_checks)
    
   
    df.reset_index(drop=True, inplace=True)
    
    
    df.loc[(df['Product Description'] == 'MILES DIESEL') | (df['Product Description'] == 'Ire Derv'), 'Product Description'] = 'DIESEL'
    df.loc[(df['Product Description'] == 'Ire AdBlue (Packaged)'), 'Product Description'] = 'AD BLUE PACK'
    df.loc[(df['Product Description'] == 'Ire AdBlue'), 'Product Description'] = 'AD BLUE'
    df.loc[(df['Product Description'] == 'Ire Car Wash'), 'Product Description'] = 'CAR WASH'
    df.loc[(df['Product Description'] == 'Ire Lub Oil') | (df['Product Description'] == 'LUBES'), 'Product Description'] = 'LUBE OIL'
    
    df.reset_index(drop=True, inplace=True)
    
    excelfilename = 'fuelCardChecks_Master.csv'
    df.to_csv(excelfilename) 
    
    return



    
fuel_card_checks = joinFuelCardCheckData(prov1, prov2, prov3)
