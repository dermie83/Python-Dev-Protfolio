from itertools import product
import pandas as pd
import numpy as np


# read in csv file from read_BAS_files.py
BAS_files = "BAS_files.csv"

try:
    with open(BAS_files, 'r',encoding="ISO-8859-1") as file:
            BAS_files_df = pd.read_csv(file, encoding="ISO-8859-1")
            BAS_files_df = pd.DataFrame(BAS_files_df)
            BAS_files_df['Presentation Date'] = pd.to_datetime(BAS_files_df['Presentation Date'], format='mixed')
except FileNotFoundError:
    print("Error: The CSV file could not be found.")

# filter out rows that are equal to PPCREDIT
BAS_files_df = BAS_files_df[BAS_files_df['Product Code'] != 'PPCREDIT']

# Drop helper columns
BAS_files_df = BAS_files_df.drop(['Next Revenue', 'Actual Revenue',
                                'New Volume', 'Next Product Code'],axis=1)

#get unique customer names
customer_name_df = BAS_files_df.drop(['Docket Number', 'Customer ID', 'Office',
                                'Ceadunas/Meter Die No', 'Acceptance Status', 'Validation Type',
                                'Docket Type', 'Presentation Date', 'Product Group', 'Product Code',
                                'Product Description', 'Mail Type', 'Volume', 'Revenue', 'Product Type',
                                'Mail Media', 'Urban', 'Sub Urban', 'Rural', 'Inward Sort Rate',
                                'Outward Sort Rate', 'Cost per Item'], axis=1)

customer_name_df.drop_duplicates(subset= ['Company Name'], inplace=True, ignore_index=True, keep='first')
customer_name_df.reset_index(drop=True, inplace=True)

# create unique customer list
customers_list = set(customer_name_df.get('Company Name').tolist())

# get max and min dates used for date range for MA metrics
start_date = BAS_files_df['Presentation Date'].max()
end_date = BAS_files_df['Presentation Date'].min()

# create empty calendar list
calendar_list = []

# create calendar dataframe with range
df_calendar = pd.DataFrame()
df_calendar['DATE'] = pd.date_range(end=start_date,start=end_date)
df_calendar['DATE'] = pd.to_datetime(df_calendar['DATE'], format='mixed')
calendar_list.append(df_calendar)

# create calendar dataframe
calendar_df = pd.concat(calendar_list, ignore_index=True)
calendar_df['YEAR'] = calendar_df['DATE'].dt.isocalendar().year
calendar_df['WEEK'] = calendar_df['DATE'].dt.isocalendar().week

# get total week count to use in the moving average class
num_rows = len(calendar_df)
rolling_days = int(num_rows)

# create a list with date range      
date_list = set(calendar_df.get('DATE').tolist())

# Match up customers with date list
customer_to_date_df = pd.DataFrame(product(date_list, customers_list), columns=['DATE', 'Company Name'])

# merge newly created customer and date range dataframe to BAS file
customer_bas = customer_to_date_df.merge(BAS_files_df, left_on = ['DATE', 'Company Name'], right_on = ['Presentation Date', 'Company Name'], how = 'left' )

# Add YEAR and WEEK columns and sort for MA metrics calculations
customer_bas['YEAR'] = customer_bas['DATE'].dt.isocalendar().year
customer_bas['WEEK'] = customer_bas['DATE'].dt.isocalendar().week
customer_bas = customer_bas.sort_values(['DATE'], ascending=True)

# Revenue and Volumn agg by product code (pc)
# If you set min_count=1, it means at least 1 non-NA value is required to calculate the sum. If all values are NA, the result will be NA.
# Also, this IS used to stop the accurance of adding zeros to where nan values are when AGG volume and Revenue
customer_bas_weekly_rev_pc = customer_bas.groupby(['Company Name','YEAR','WEEK','Account Managers', 
                                                   'Product Code', 'Product Description', 'Mail Type'], as_index=False)[['Volume','Revenue']].sum(min_count=1)

# Add zeros to where blanks rows occur but only after first instance of a value (Revenue exchange) - this is to ensure continuity for the moving averages calculations
# customer_bas_weekly_rev_pc['Revenue'] = customer_bas_weekly_rev_pc.groupby(['Company Name'])['Revenue'].transform(lambda x: x.fillna(x.mask(x.ffill().notna(),0)))


# # detect outliers and remove to reduce right skewe-ness of data (also effective in removing seasonality changes)
def detect_outliers_iqr_for_revenue(df, group_column1, group_column2, value_column, scale_factor=4):
        '''This function uses the IQR method to detect outliers that will fall outside of the
            upper and lower limits that is set by the scale factor, also effective in removing seasonality changes
        '''
        # Calculate Q1 (25th percentile) and Q3 (75th percentile) for each group
        Q1 = df.groupby([group_column1, group_column2])[value_column].quantile(0.25)
        Q3 = df.groupby([group_column1, group_column2])[value_column].quantile(0.75)
        
        # Calculate IQR (Interquartile Range) for each group
        IQR = Q3 - Q1
        
        
        # Merge lower_bound back into the original dataframe to align it with each row
        df['lower_bound'] = df.groupby([group_column1, group_column2])[value_column].transform(lambda x: Q1.loc[x.name] - scale_factor * IQR.loc[x.name])
        df['upper_bound'] = df.groupby([group_column1, group_column2])[value_column].transform(lambda x: Q1.loc[x.name] + scale_factor * IQR.loc[x.name])
        
        # Detect outliers: values below the lower bound are considered outliers
        outliers = (df[value_column] < df['lower_bound']) | (df[value_column] > df['upper_bound'])

        
        # # Optionally, drop the 'lower_bound' column if it's no longer needed
        df.drop(columns='lower_bound', inplace=True)
        df.drop(columns='upper_bound', inplace=True)
        
        
        return outliers

# apply the function for each customer group  and filter out the outliers
outliers = detect_outliers_iqr_for_revenue(customer_bas_weekly_rev_pc,'Company Name','Product Code', 'Revenue')
customer_bas_weekly_rev_pc['outlier'] = outliers
customer_bas_weekly_rev_pc = customer_bas_weekly_rev_pc[customer_bas_weekly_rev_pc['outlier'] != True]
customer_bas_weekly_rev_pc = customer_bas_weekly_rev_pc.drop(['outlier'], axis=1)

# get last day of volume injection by customer and how long ago the injection occured
customer_last_bas_inj = customer_bas
customer_bas['MAX_DATE'] = start_date
customer_bas['MIN_DATE'] = end_date
customer_bas.loc[customer_bas['Presentation Date'].isna(), 'Presentation Date'] = end_date
customer_bas['LAST_BAS_INJECTION_DAYS'] = (customer_bas['MAX_DATE'] - customer_bas['Presentation Date']).dt.days
customer_bas['LAST_BAS_INJECTION_DAYS'] = np.ceil(customer_bas['LAST_BAS_INJECTION_DAYS'] / 10) * 10

# get the most recent date
customer_bas_last_inj = customer_bas.loc[customer_bas.groupby(['Company Name', 'Product Code'])['Presentation Date'].idxmax()]

# drop redundent columns
customer_bas_last_inj = customer_bas_last_inj.drop(['Product Type', 'Volume', 'Revenue', 'Mail Type','Account Managers',
                                                        'Mail Media', 'Urban', 'Sub Urban', 'Rural', 'Inward Sort Rate',
                                                        'Outward Sort Rate', 'Cost per Item','Product Description',
                                                        'Mail Type', 'MAX_DATE', 'MIN_DATE', 'YEAR','WEEK'], axis=1)


# Customer MA of Revenue by product Code - without outliers
customer_bas_weekly_rev_pc['MA_Rev_Product_Code'] = customer_bas_weekly_rev_pc.groupby(['Company Name', 'Product Code'])['Revenue'].transform(lambda x: x.rolling(rolling_days+1, 3).mean())
customer_bas_weekly_rev_pc['Next_MA_Rev_Product_Code'] = customer_bas_weekly_rev_pc.groupby(['Company Name', 'Product Code'])['MA_Rev_Product_Code'].shift(-1)
customer_bas_weekly_rev_pc['MA_Var_Rev_Product_Code'] = customer_bas_weekly_rev_pc['Next_MA_Rev_Product_Code'] - customer_bas_weekly_rev_pc['MA_Rev_Product_Code']
customer_bas_weekly_rev_pc['MA_Var_%_Rev_Product_Code'] = ((customer_bas_weekly_rev_pc['MA_Var_Rev_Product_Code'])/abs(customer_bas_weekly_rev_pc['MA_Rev_Product_Code'])).replace(np.inf, 0)
customer_bas_weekly_rev_pc['MA_Var_%_Rev_Product_Code'] = customer_bas_weekly_rev_pc.groupby(['Company Name', 'Product Code'])['MA_Var_%_Rev_Product_Code'].shift(1)

# Create helper columns thta enable trend identification and calculations
customer_bas_weekly_rev_pc['first'] = customer_bas_weekly_rev_pc.groupby(['Company Name', 'Product Code'])['MA_Rev_Product_Code'].transform('first')
customer_bas_weekly_rev_pc['last'] = customer_bas_weekly_rev_pc.groupby(['Company Name', 'Product Code'])['MA_Rev_Product_Code'].transform('last')
customer_bas_weekly_rev_pc['MA_%_CHANGE_TOTAL'] = (customer_bas_weekly_rev_pc['last'] - customer_bas_weekly_rev_pc['first'])/customer_bas_weekly_rev_pc['first']
customer_bas_weekly_rev_pc['MA_TREND'] = ""
customer_bas_weekly_rev_pc.loc[(customer_bas_weekly_rev_pc['MA_%_CHANGE_TOTAL'] < 0) , 'MA_TREND'] = 'Decreasing'
customer_bas_weekly_rev_pc.loc[(customer_bas_weekly_rev_pc['MA_%_CHANGE_TOTAL'] > 0) , 'MA_TREND'] = 'Increasing'
customer_bas_weekly_rev_pc.loc[(customer_bas_weekly_rev_pc['MA_%_CHANGE_TOTAL'] == 0) | (customer_bas_weekly_rev_pc['MA_%_CHANGE_TOTAL'].isna()), 'MA_TREND'] = 'No Trend'

# merge MA metrics with trend identification columns 
customer_bas_weekly_rev_pc = customer_bas_weekly_rev_pc.merge(customer_bas_last_inj, left_on = ['Company Name', 'Product Code'], right_on = ['Company Name', 'Product Code'], how = 'left' )

# Drop redundent columns
customer_bas_weekly_rev_pc = customer_bas_weekly_rev_pc.drop(['Next_MA_Rev_Product_Code','MA_Var_Rev_Product_Code','first', 'last',
       'DATE', 'Docket Number', 'Customer ID',
       'Office', 'Ceadunas/Meter Die No', 'Acceptance Status',
       'Validation Type', 'Docket Type'], axis=1)

# this is the dataset used in Power BI dashboard
customer_bas_weekly_rev_pc.to_csv('output_file.csv')
