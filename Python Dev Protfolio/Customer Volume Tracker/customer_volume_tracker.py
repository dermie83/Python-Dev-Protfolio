from datetime import datetime
from itertools import product
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np



# read in file generated from append_all_file.py
csv_file = "customer_volume_files.csv"

try:
        with open(csv_file, 'r',encoding="ISO-8859-1") as file:
                df = pd.read_csv(file, encoding="ISO-8859-1")
                df = pd.DataFrame(df)
                df = df.replace('', np.nan).dropna()

                # this parameter is used in the apply_check_and_filter (the higher the number the longer it will take the script to complete)
                iterations = 60

                # Get unique customers
                customer_df = df.drop(['COUNT(BARCODE)','SCAN_DATE','YEAR','WEEK'], axis=1)
                customer_df.drop_duplicates(subset= ['CL_CUSTOMER_ID'], inplace=True, ignore_index=True, keep='first')
                customer_df.reset_index(drop=True, inplace=True)
                # create unique customer list that will be used to match customer to date range
                customers_list = set(customer_df.get('CL_CUSTOMER_ID').tolist())

                # format SCAN_DATE column, get max year and and same day but 4 years ago (The years parameter is optional)
                df['SCAN_DATE'] = pd.to_datetime(df['SCAN_DATE'], format='mixed')
                start_date = df['SCAN_DATE'].max()
                end_date = df['SCAN_DATE'].max() - relativedelta(years=4)
                
                # empty list
                volume_year_list = []
                calendar_list = []

                # Filter volume dataset - only include date range set between start and end dates
                df_ = df.loc[(df['SCAN_DATE'] <= start_date) & (df['SCAN_DATE'] >= end_date)]
                volume_year_list.append(df_)
                volume_df = pd.concat(volume_year_list, ignore_index=True)

                # create calendar with full range - set from yearly parameters
                df_calendar = pd.DataFrame()
                df_calendar['DATE'] = pd.date_range(end=start_date,start=end_date)
                df_calendar['DATE'] = pd.to_datetime(df_calendar['DATE'], format='mixed')
                df_calendar.drop_duplicates(inplace=True, ignore_index=True)
                calendar_list.append(df_calendar)

                # create calendar dataframe from calendar list - used for matching customer to entire calendar list
                calendar_df = pd.concat(calendar_list, ignore_index=True)
                date_list = set(calendar_df.get('DATE').tolist())

                # get total week count that to use in the moving average class
                num_rows = len(calendar_df)
                rolling_days = int(num_rows)

                # create a hypothetical account managers list
                am_list = ['AM1','AM2','AM3','AM4','AM5','AM6','AM7']
                customer = volume_df['CL_CUSTOMER_ID'].unique()
                # Randomly assign each unique value a label from the list
                random_labels = np.random.choice(am_list, size=len(customer), replace=True)
                # Create a dictionary to map unique categories to random labels
                category_to_label = dict(zip(customer, random_labels))
                # Assign the new 'Label' column based on the mapping
                volume_df['Account Managers'] = volume_df['CL_CUSTOMER_ID'].map(category_to_label)

                # List of IDs to match account type
                not_sme_list = [-1]

                # Create the new column based on the condition
                volume_df['ACCOUNT_TYPE'] = volume_df['CL_CUSTOMER_ID'].apply(lambda x: 'NOT SME' if x in not_sme_list else 'SME')
                
                
                # this dataset is used in the power BI dashbaord
                volume_df.to_csv('filtered_dataset_files.csv')


#################################################### This section creates the MA metrics ############################################################################################

                # drop YEAR, WEEK colummns
                volume_df = volume_df.drop(['YEAR','WEEK'], axis=1)

                # create a dataframe that matches each customer account to every day from the claendar list
                customer_to_date_df = pd.DataFrame(product(date_list, customers_list), columns=['DATE', 'CL_CUSTOMER_ID'])

                # merge the customer to date datframe to the volume df (fitered_dataset)
                customer_activity = customer_to_date_df.merge(volume_df, left_on = ['DATE', 'CL_CUSTOMER_ID'], right_on = ['SCAN_DATE', 'CL_CUSTOMER_ID'], how = 'left' )

                # add YEAR and WEEK columns and sort to allows for Moving averages calculation
                customer_activity['YEAR'] = customer_activity['DATE'].dt.isocalendar().year
                customer_activity['WEEK'] = customer_activity['DATE'].dt.isocalendar().week
                customer_activity = customer_activity.sort_values(['DATE'], ascending=True)

                # optional selection of date range for MA datasets  - this can be changed to include any YEAR or WEEK within list
                customer_activity = customer_activity[customer_activity['YEAR'].isin([2024, 2025])]

                # # Customer weekly MA metrics
                # If you set min_count=1, it means at least 1 non-NA value is required to calculate the sum. If all values are NA, the result will be NA.
                # Also, this IS used to stop the accurance of adding zeros to where nan values are when AGG volume
                customer_activity_weekly = customer_activity.groupby(['CL_CUSTOMER_ID','YEAR','WEEK'], as_index=False)['COUNT(BARCODE)'].sum(min_count=1)

                # Add zeros to where blanks rows occur but only after first instance of a value (injected  volume) - this is to ensure continuity for the moving averages calculations
                # customer_activity_weekly['COUNT(BARCODE)'] = customer_activity_weekly.groupby('CL_CUSTOMER_ID')['COUNT(BARCODE)'].transform(lambda x: x.fillna(x.mask(x.ffill().notna(),0)))

                # Add customer names to each of their corrrosponding ID's
                customer_activity_weekly = customer_activity_weekly.merge(customer_df, left_on = ['CL_CUSTOMER_ID'], right_on = ['CL_CUSTOMER_ID'], how = 'left' )


                def detect_outliers_iqr_for_volume(df, group_column1, value_column, scale_factor=2):
                        '''This function uses the IQR method to detect outliers that will fall outside of the
                          upper and lower limits that is set by the scale factor, also effective in removing seasonality changes
                        '''
                        # Calculate Q1 (25th percentile) and Q3 (75th percentile) for each group
                        Q1 = df.groupby([group_column1])[value_column].quantile(0.25)
                        Q3 = df.groupby([group_column1])[value_column].quantile(0.75)
                        
                        # Calculate IQR (Interquartile Range) for each group
                        IQR = Q3 - Q1
                        
                        # Merge lower_bound back into the original dataframe to align it with each row
                        df['lower_bound'] = df.groupby([group_column1])[value_column].transform(lambda x: Q1.loc[x.name] - scale_factor * IQR.loc[x.name])
                        df['upper_bound'] = df.groupby([group_column1])[value_column].transform(lambda x: Q1.loc[x.name] + scale_factor * IQR.loc[x.name])
                        
                        # Detect outliers: values below the lower bound are considered outliers
                        outliers = (df[value_column] < df['lower_bound']) | (df[value_column] > df['upper_bound'])

                        # Optionally, drop the 'lower_bound' column if it's no longer needed
                        df.drop(columns='lower_bound', inplace=True)
                        df.drop(columns='upper_bound', inplace=True)
                        
                        return outliers

                # call the function, create a new column ['outliers] and filter out any rows that equals true (indicate outliers)
                outliers = detect_outliers_iqr_for_volume(customer_activity_weekly, 'CL_CUSTOMER_ID', 'COUNT(BARCODE)')
                customer_activity_weekly['outlier'] = outliers
                customer_activity_weekly = customer_activity_weekly[customer_activity_weekly['outlier'] != True]
                
                
                # # helper function to get rid of inf calc 
                # def replace_first_zero(group):
                # # Check if the first instance of 0 exists in the group
                #         if (group['COUNT(BARCODE)'] == 0).any():
                #                 # Find the index of the first 0 in the 'value' column
                #                 first_zero_index = group[group['COUNT(BARCODE)'] == 0].index[0]
                #                 # Replace it with 1
                #                 group.at[first_zero_index, 'COUNT(BARCODE)'] = 1
                #         return group

                # # Apply the function to each group
                # customer_activity_weekly_1 = customer_activity_weekly.groupby('CL_CUSTOMER_ID', group_keys=False).apply(replace_first_zero)

                # Create a Helper column MA_1 for Volume - this is iteration 1. This will help remove other outliers that are present
                customer_activity_weekly['MA_1'] = customer_activity_weekly.groupby(['CL_CUSTOMER_ID'])['COUNT(BARCODE)'].transform(lambda x: x.rolling(rolling_days+1, 3).mean())

                def check_80_percent_bigger(group):
                        '''Function to check if the value is 80% bigger than the previous row for each group.
                        This is remove rows woth very low volume that are the result of testing and on-boarding'''

                        group = group.reset_index(drop=True)  # Reset the index to ensure it's continuous from 0
                        results = [False]  # First row has no previous row, so it can't be 80% bigger.
                        
                        # Initialize a while loop to iterate through the group rows
                        i = 1
                        while i < len(group):
                                # Skip NaN values and move to the next row if found
                                if pd.isna(group.loc[i, 'MA_1']) or pd.isna(group.loc[i-1, 'MA_1']):
                                        results.append(False)  # No comparison if either value is NaN
                                else:
                                # Compare the current value with the previous value (if 80% bigger)
                                        if group.loc[i, 'MA_1'] > 1.8 * group.loc[i-1, 'MA_1']:  # 80% bigger
                                                results.append(True)
                                        else:
                                                results.append(False)
                                i += 1
                        
                        # Add the results as a new column
                        group['is_80_percent_bigger'] = results
                        return group
                
                
                def apply_check_and_filter(df, group_column, iterations=5):
                        '''This function checks if the rows are 80% bigger (true)
                        and filters the true values out. The amount of iterations are depended on how many 
                        rows are considered insignificant rows - default set at 5 but 
                        over 50 should be enough'''
                        for i in range(iterations):
                                # Apply the function to each group using groupby
                                df = df.groupby(group_column, group_keys=False).apply(check_80_percent_bigger)
                                
                                # Create a helper column shifted by -1
                                helper_column = f'is_80_percent_bigger_helper_{i}'  # Unique name for each iteration
                                df[helper_column] = df.groupby([group_column])[ 'is_80_percent_bigger'].shift(-1)
                                
                                #Filter out rows where the helper column is True
                                df = df[df[helper_column] != True]
                                
                        return df

                # Apply the function with 5 iterations
                customer_activity_weekly_1 = apply_check_and_filter(customer_activity_weekly, 'CL_CUSTOMER_ID', iterations = iterations)
                customer_activity_weekly_1 = customer_activity_weekly_1.reset_index(drop=True)


                # get the last volume injected total and the date it occured
                customer_activity['MAX_DATE'] = start_date
                customer_activity['MIN_DATE'] = end_date
                customer_activity.loc[customer_activity['SCAN_DATE'].isna(), 'SCAN_DATE'] = end_date
                customer_activity['LAST_INJECTION_DAYS'] = (customer_activity['MAX_DATE'] - customer_activity['SCAN_DATE']).dt.days
                customer_activity['LAST_INJECTION_DAYS'] = np.ceil(customer_activity['LAST_INJECTION_DAYS'] / 10) * 10

                # get the most recent date
                customer_last_activity = customer_activity.loc[customer_activity.groupby('CL_CUSTOMER_ID')['SCAN_DATE'].idxmax()]


                # remove un-used columns
                customer_last_activity = customer_last_activity.drop(['SCAN_DATE', 'YEAR', 'WEEK', 'CL_CUSTOMER_NAME',
                                                                        'MAX_DATE', 'MIN_DATE','ACCOUNT_TYPE'], axis=1)
                
                
                # rename column header for dashboard and clarity
                customer_last_activity = customer_last_activity.rename(columns={'COUNT(BARCODE)': 'LAST DAILY VOL INJ'})


                # Customer MA of Volume - iteration 2 (All Outliers POSSIBLY removed at this point) A True MA calculation should be calculated
                customer_activity_weekly_1['MA_Vol'] = customer_activity_weekly_1.groupby(['CL_CUSTOMER_ID'])['COUNT(BARCODE)'].transform(lambda x: x.rolling(rolling_days+1, 3).mean())
                customer_activity_weekly_1['Next_Week_MA_Vol'] = customer_activity_weekly_1.groupby(['CL_CUSTOMER_ID'])['MA_Vol'].shift(-1)
                customer_activity_weekly_1['MA_Weekly_Var_Vol'] = customer_activity_weekly_1['Next_Week_MA_Vol'] - customer_activity_weekly_1['MA_Vol']
                customer_activity_weekly_1['MA_Weekly_Var_%_Vol'] = ((customer_activity_weekly_1['MA_Weekly_Var_Vol'])/abs(customer_activity_weekly_1['MA_Vol'])).replace(np.inf, 0)
                customer_activity_weekly_1['MA_Weekly_Var_%_Vol'] = customer_activity_weekly_1.groupby(['CL_CUSTOMER_ID'])['MA_Weekly_Var_%_Vol'].shift(1)

                # Create helper columns thta enable trend identification and calculations
                customer_activity_weekly_1['first'] = customer_activity_weekly_1.groupby('CL_CUSTOMER_ID')['MA_Vol'].transform('first')
                customer_activity_weekly_1['last'] = customer_activity_weekly_1.groupby('CL_CUSTOMER_ID')['MA_Vol'].transform('last')
                customer_activity_weekly_1['MA_%_CHANGE_TOTAL'] = (customer_activity_weekly_1['last'] - customer_activity_weekly_1['first'])/customer_activity_weekly_1['first']
                customer_activity_weekly_1['MA_TREND'] = ""
                customer_activity_weekly_1.loc[(customer_activity_weekly_1['MA_%_CHANGE_TOTAL'] < 0) , 'MA_TREND'] = 'Decreasing'
                customer_activity_weekly_1.loc[(customer_activity_weekly_1['MA_%_CHANGE_TOTAL'] > 0) , 'MA_TREND'] = 'Increasing'
                customer_activity_weekly_1.loc[(customer_activity_weekly_1['MA_%_CHANGE_TOTAL'] == 0), 'MA_TREND'] = 'No Trend'
                customer_activity_weekly_1.loc[(customer_activity_weekly_1['MA_%_CHANGE_TOTAL'].isna()), 'MA_TREND'] = 'BLANK'

                customer_activity_weekly_1 = customer_activity_weekly_1.merge(customer_last_activity, left_on = ['CL_CUSTOMER_ID'], right_on = ['CL_CUSTOMER_ID'], how = 'left' )
               
                # apply the account type (SME or Not to dataset)
                customer_1 = customer_activity_weekly_1['CL_CUSTOMER_ID'].unique()
                # Randomly assign each unique value a label from the list
                random_labels_1 = np.random.choice(am_list, size=len(customer_1), replace=True)
                # Create a dictionary to map unique categories to random labels
                category_to_label_1 = dict(zip(customer_1, random_labels_1))
                # Assign the new 'Label' column based on the mapping
                customer_activity_weekly_1['Account Managers'] = customer_activity_weekly_1['CL_CUSTOMER_ID'].map(category_to_label_1)
                customer_activity_weekly_1['ACCOUNT_TYPE'] = customer_activity_weekly_1['CL_CUSTOMER_ID'].apply(lambda x: 'NOT SME' if x in not_sme_list else 'SME')


                def drop_helper_columns(df: pd.DataFrame, max_index: int = 5):
                        # Create a list of columns to drop
                        columns_to_drop = ['outlier', 'Next_Week_MA_Vol', 'MA_Weekly_Var_Vol',
                                           'first', 'last', 'is_80_percent_bigger','MA_1']
                        
                        # Add the helper columns (from 0 to max_index)
                        for i in range(max_index + 1):
                                columns_to_drop.append(f'is_80_percent_bigger_helper_{i}')
                        
                        # Drop the columns from the DataFrame
                        df = df.drop(columns=columns_to_drop, axis=1)
                        
                        return df
                # apply the funciton to the dataframe
                customer_activity_weekly_1 = drop_helper_columns(customer_activity_weekly_1, max_index = iterations-1)
                
               
                # This dataset is used in the Power BI dashboard
                customer_activity_weekly_1.to_csv('customer_MA_weekly_files.csv')

        today = datetime.today()
        print(".............this script may take up to 30 minutes to complete. Script completed at....",today)
       
except FileNotFoundError:
        print("Error: The CSV file could not be found.")

