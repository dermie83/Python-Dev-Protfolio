import pandas as pd
import numpy as np


depots = pd.read_excel(r'Exited_Entered - Copy.xlsx', sheet_name = 'Exited_Entered')

locations_df = pd.DataFrame(depots)
#print(locations_df)
locations_df = locations_df[locations_df['Verizon Locations'] != 'DSU-Athlone']
locations_df.reset_index()
#locations_df.to_csv('locations_df.csv')

# add columns with radians for latitude and longitude
locations_df[['lat_radians_loc','long_radians_loc']] = (
    np.radians(locations_df.loc[:,['Latitude','Longitude']]))



depots_1 = pd.read_excel(r'Exited_Entered - Copy.xlsx', sheet_name = 'Locations')

locations_df_1 = pd.DataFrame(depots_1)

locations_df_1 = locations_df_1[locations_df_1['Verizon Locations'] != 'DSU-Athlone']
#locations_df_1.to_csv('locations_df_1.csv')

# add columns with radians for latitude and longitude
locations_df_1[['lat_radians_loc','long_radians_loc']] = (
    np.radians(locations_df_1.loc[:,['Latitude','Longitude']]))




dummy_file = pd.read_excel(r'dummy_file.xlsx')
dummy_df = pd.DataFrame(dummy_file)
#print(dummy_df)



# serviceStations = locations_df[locations_df['Depot Type'] == 'Service Centres']


# print(serviceStations['Verizon Locations'].unique())