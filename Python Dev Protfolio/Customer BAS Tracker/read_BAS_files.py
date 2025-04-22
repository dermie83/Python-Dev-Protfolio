# import necessary libraries 
import os
import pandas as pd
import glob
import csv
import numpy as np

# Get a list of all excel files in a directory
BAS_files = glob.glob('*.xlsx')
print(BAS_files)
# Create an empty dataframe to store the combined data
BAS_files_df = pd.DataFrame()

for BAS_file in BAS_files:
    print(BAS_file)
    try:
        # Check if file exists before attempting to read
            if not os.path.exists(BAS_file):
                raise FileNotFoundError(f"File not found: {BAS_file}")
            
            # Try to read the Excel file into a pandas DataFrame
            df = pd.read_excel(BAS_file, skiprows=10) 
            BAS_files_df = pd.concat([BAS_files_df, df])

            # data prep and and helper columns created
            BAS_files_df.loc[BAS_files_df['Mail Type'].isna(), 'Mail Type'] = 'Other'
            BAS_files_df['Total €'] = BAS_files_df['Total €'].replace({',': ''}, regex=True).astype(float)
            BAS_files_df['Volume'] = BAS_files_df['Volume'].replace({',': ''}, regex=True).astype(float)
            BAS_files_df['Next Revenue'] = BAS_files_df.groupby(['Customer ID'])['Total €'].shift(-1)
            BAS_files_df['Next Product Code'] = BAS_files_df.groupby(['Customer ID'])['Product Code'].shift(-1)

            # add a hypothical list of account managers
            am_list = ['AM1','AM2','AM3','AM4','AM5','AM6','AM7']
            company = BAS_files_df['Company Name'].unique()
            # Randomly assign each unique value a label from the list
            random_labels = np.random.choice(am_list, size=len(company), replace=True)
            # Create a dictionary to map unique categories to random labels
            category_to_label = dict(zip(company, random_labels))
            # Assign the new 'Label' column based on the mapping
            BAS_files_df['Account Managers'] = BAS_files_df['Company Name'].map(category_to_label)
            # print(BAS_files_df.dtypes)

           
    except FileNotFoundError:
        print("Error: The excel file could not be found.")
    except csv.Error as e:
        print(f"Error: {e}")
    except UnicodeDecodeError:
        print("Error: The excel file contains characters that cannot be decoded.")
    except Exception as e:
        print(f"Unexpected error: {e}")

# more data prep
BAS_files_df.rename(columns={'Total €': 'Revenue'}, inplace=True)
BAS_files_df.reset_index(drop=True, inplace=True)
BAS_files_df['Cost per Item'] = ((BAS_files_df['Revenue'])/(BAS_files_df['Volume'])).replace(np.inf, 0)
BAS_files_df['Actual Revenue'] = BAS_files_df['Revenue']+BAS_files_df['Next Revenue']
BAS_files_df['New Volume'] = (BAS_files_df['Actual Revenue'])/(BAS_files_df['Cost per Item'])
# Update Revenue and Volume values to true values that are effected by a return in credit
BAS_files_df['Revenue'] = BAS_files_df.apply(lambda row: row['Actual Revenue'] if row['Next Product Code'] == 'PPCREDIT' else row['Revenue'], axis=1)
BAS_files_df['Volume'] = BAS_files_df.apply(lambda row: row['New Volume'] if row['Next Product Code'] == 'PPCREDIT' else row['Volume'], axis=1)
BAS_files_df.to_csv('BAS_files.csv',index=False)
print(f"Successfully created new dataframe {BAS_files_df}")



