# import necessary libraries 
import pandas as pd
import glob
import csv

# Get a list of all CSV files in a directory
csv_files = glob.glob('*.csv')
# print(csv_files)
# Create an empty dataframe to store the combined data
csv_files_df = pd.DataFrame()

# Loop through each CSV file and append its contents to the combined dataframe
for csv_file in csv_files:
    try:
        with open(csv_file, 'r',encoding="ISO-8859-1") as file:
            df = pd.read_csv(file, encoding="ISO-8859-1")
            csv_files_df = pd.concat([csv_files_df, df])
            csv_files_df['SCAN_DATE'] = pd.to_datetime(csv_files_df['SCAN_DATE'], format='mixed')
            csv_files_df['YEAR'] = csv_files_df['SCAN_DATE'].dt.isocalendar().year
            csv_files_df['WEEK'] = csv_files_df['SCAN_DATE'].dt.isocalendar().week

            # clean dataframe
            csv_files_df.loc[(csv_files_df['CL_CUSTOMER_NAME'] == 'Unknown') ,'CL_CUSTOMER_ID'] = 777
            csv_files_df = csv_files_df.drop(['CL_DAY_DESC', 'CL_FIN_WEEK_DESC', 'CL_FIN_YEAR'], axis=1)
            print("csv_files.....",csv_files_df)
           
    except FileNotFoundError:
        print("Error: The CSV file could not be found.")
    except csv.Error as e:
        print(f"Error: {e}")
    except UnicodeDecodeError:
        print("Error: The CSV file contains characters that cannot be decoded.")
    except Exception as e:
        print(f"Unexpected error: {e}")

csv_files_df.to_csv('customer_volume_files.csv',index=False)


    