from tkinter import *
import tkinter as tk
from tkinter import ttk
import datetime as dt
from tkinter import messagebox
from tkinter import filedialog
from tkinter.constants import RIGHT, VERTICAL, HORIZONTAL, BOTTOM, X, Y, BOTH, END
from tkinter.filedialog import askopenfile
import pandas as pd
import numpy as np
from sklearn import *
from sklearn.metrics import DistanceMetric
#from sklearn.neighbors import DistanceMetric
from locations import locations_df, dummy_df
from re import search
from PIL import Image, ImageTk
import os
from datetime import datetime


roots = tk.Tk()
roots.title("An Post Vehicle Location Snapshot v1.1")
roots.geometry('1500x650')

frame = tk.Frame(roots)
frame.pack(fill = BOTH, expand = True)

# Add image file

# resized_image = bg.resize((1900,1000), Image.ANTIALIAS)
# new_image = ImageTk.PhotoImage(resized_image)

query_canvas = tk.Canvas(frame)
query_canvas.pack(fill = BOTH, expand = True)

# # Display image
# query_canvas.create_image( 0, 0, image = new_image, anchor = "nw")

# Create a Treeview Frame
tree_frame = tk.Frame(query_canvas)
tree_frame.pack(pady=10)
btn_frame = tk.Frame(query_canvas, bg = 'light blue')
btn_frame.pack(pady=10)


# Create a Treeview Scrollbar
tree_scroll_yaxis = tk.Scrollbar(tree_frame,orient=VERTICAL)
tree_scroll_yaxis.pack(side=RIGHT, fill = Y)
tree_scroll_xaxis = tk.Scrollbar(tree_frame, orient=HORIZONTAL)
tree_scroll_xaxis.pack(side=BOTTOM, fill=X)


# Create The Treeview
my_tree = ttk.Treeview(tree_frame, yscrollcommand=tree_scroll_yaxis.set, selectmode="extended")
my_tree = ttk.Treeview(tree_frame, xscrollcommand=tree_scroll_xaxis.set, selectmode="extended")
my_tree.pack()

# Configure the Scrollbar
tree_scroll_yaxis.config(command=my_tree.yview)
tree_scroll_xaxis.config(command=my_tree.xview)


def clear_new_file_entry_boxes():
    new_file_ent.delete(0,END)


def clear_file_ent_entry_boxes():
    file_ent.delete(0,END)


def clear_all_entry_boxes():
    clear_new_file_entry_boxes()
    clear_file_ent_entry_boxes()


def clear_treeview():
    for i in my_tree.get_children():
        my_tree.delete(i)
        

def check_df(x,y):
    try:
        x.columns == y.columns
        return True
    except:  
       return False
   
   
def open_file():
    clear_file_ent_entry_boxes()
    file = filedialog.askopenfilename()
    file_ent.insert(END, file)   
   
   
def get_locations():
   global df
   data = file_ent.get()
   ext = os.path.splitext(data)[1]
   if data == "":
       clear_all_entry_boxes()
       messagebox.showinfo("Error!","Please upload a file 'Daily Report' file!")
   if ext == '.csv':
       read_csv = pd.read_csv(data, encoding= 'unicode_escape')
       df = pd.DataFrame(read_csv)
      
       # df["Arrival Time"] = pd.to_datetime(df['Arrival Time'], format='%H:%M:%S')
       # df['Arrival Time'] = df['Arrival Time'].dt.strftime('%H:%M:%S')
       # df["Date"] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
       check = check_df(df,dummy_df)
       if check == True:
           daily_report_file_setup(df)
           create_new_csv_file_name()
       else:
           messagebox.showinfo("Error!","Wrong file type! Please use a Verizon 'Daily Report' file.")
   elif ext == '.xlsx':
       read_excel = pd.read_excel(data)
       df = pd.DataFrame(read_excel)
       check = check_df(df,dummy_df)
       if check == True:  
           daily_report_file_setup(df)
           create_new_csv_file_name()
       else:
           messagebox.showinfo("Error!","Wrong file type! Please use 'Daily Report' file.")
          
               
def daily_report_file_setup(df):
    df[['lat_radians_df','long_radians_df']] = (np.radians(df.loc[:,['Stop Latitude','Stop Longitude']]))
    
    df["Arrival Time"] = pd.to_datetime(df['Arrival Time'], format='%H:%M:%S')
    df['Arrival Time'] = df['Arrival Time'].dt.strftime('%H:%M:%S')
    df["Date"] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    
    
    df['Arrival Date_Time'] = pd.to_datetime(df["Date"].astype(str)+" "+df["Arrival Time"].astype(str), format='%Y/%m/%d %H:%M:%S')
    
    df = df.groupby('Vehicle Name', sort=False).apply(lambda x: x[x['Arrival Date_Time'] == x['Arrival Date_Time'].max()]).reset_index(drop=True)
    date = dt.datetime.now()
    df['Snapshot File Created'] = date
    df['Snapshot File Created'] = df['Snapshot File Created'].dt.strftime('%Y/%m/%d %H:%M:%S')
    df["Snapshot File Created"] = pd.to_datetime(df['Snapshot File Created'], format='%Y/%m/%d %H:%M:%S')
    
    return df


def create_unit_numbers_at_location(df):
    dist = DistanceMetric.get_metric('haversine')
    dist_matrix = (dist.pairwise(locations_df[['lat_radians_loc','long_radians_loc']], df[['lat_radians_df','long_radians_df']])*6367)
    df_dist_matrix = pd.DataFrame(dist_matrix,index=locations_df['Verizon Locations'], columns=df['Vehicle Name'])
    # Unpivot this dataframe from wide format to long format.
    df_dist_long = (pd.melt(df_dist_matrix.reset_index(),id_vars='Verizon Locations'))
    df_dist_long = df_dist_long.rename(columns={'value':'Kilometers'})
    #print(df_dist_long)
    # selecting rows based on condition
    df_dist_long = df_dist_long.groupby('Vehicle Name', sort=False).apply(lambda x: x[x['Kilometers'] == x['Kilometers'].min()]).reset_index(drop=True)
    grouped_df = df_dist_long.groupby("Verizon Locations")
    grouped_df = grouped_df.agg({"Vehicle Name": "nunique"}).reset_index()
    grouped_df.rename(columns={"Vehicle Name":"Total Units"},inplace=True)
    
    # define columns
    cols = list(grouped_df.columns)
    my_tree["columns"] = cols
    for i in cols:
        my_tree.column(i, anchor="w")
        my_tree.heading(i, text=i, anchor='w')
    for index, row in grouped_df.iterrows():
        my_tree.insert("",0,text=index,values=list(row))
        
  
def create_new_locations_file():
    clear_treeview()
    create_new_csv_file_name()
    get_locations()
    create_unit_numbers_at_location(df)
   
    
def check_new_entry():
    while True:
        try:
            data = new_file_ent.get()
            if data != "":
                messagebox.showinfo("Thank You!", "File Saved!")
                if 'OK':
                    break
            if data == "":
                messagebox.showinfo("Error!","Please provide 'Daily Report' file.")
                if 'OK':
                    break
        finally:
            break
            
    
def create_new_csv_file_name():
    clear_new_file_entry_boxes()
    global excelfilename
    today=datetime.today()
    TodaysDate = today.strftime("%d-%m-%Y")
    excelfilename = "fleet unit current location "+ str(TodaysDate) +".csv"
    new_file_ent.insert(END, excelfilename)


def save_to_csv():
    data = file_ent.get()
    if search('.csv', data):
        read_file = pd.read_csv(data, encoding = 'unicode_escape')
        df = pd.DataFrame(read_file)
        df1 = daily_report_file_setup(df)
        df1.to_csv(excelfilename)
        check_new_entry()
    if search('.xlsx', data):
        read_file = pd.read_excel(data)
        df = pd.DataFrame(read_file)
        df1 = daily_report_file_setup(df)
        df1.to_csv(excelfilename)
        check_new_entry()
       
       

###################################################################################################

# clear_treeview_btn = tk.Button(btn_frame, text = 'Clear contents from Table', command = clear_treeview)
# clear_treeview_btn.grid(row=0,column=1, padx=20, pady=20)


file_ent = tk.Entry(btn_frame, width = 90, font=30)
file_ent.grid(row=2,column=1)
file_ent.bind("<Key>", lambda a: "break")


open_btn = tk.Button(btn_frame, text ='Import Daily Report File', command = lambda:open_file())
open_btn.grid(row=2,column=0, padx=20, pady=20)


get_btn = tk.Button(btn_frame, text ='Create Location Table', command = create_new_locations_file)
get_btn.grid(row=3,column=0, padx=20, pady=20)


clear_entry_btn = tk.Button(btn_frame, text = 'Clear Entry Boxes', command = clear_all_entry_boxes)
clear_entry_btn.grid(row=3,column=1, padx=20, pady=20)


save_to_csv_btn = tk.Button(btn_frame, text ='Save to CSV', command = lambda:save_to_csv())
save_to_csv_btn.grid(row=4,column=0, padx=20, pady=20)

new_file_ent = tk.Entry(btn_frame, width = 90, font=30)
new_file_ent.grid(row=4,column=1)
new_file_ent.bind("<Key>", lambda a: "break")


quit_btn = tk.Button(btn_frame, text = 'Exit', command = roots.destroy)
quit_btn.grid(row=5,column=0, padx=20, pady=20)


roots.mainloop()
