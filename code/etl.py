import pandas as pd
import numpy as np
import urllib.request
import zipfile
import random
import itertools
import math
import geopandas as gpd
import pathlib


# Importing sqlalchemy package
from sqlalchemy import create_engine

# Creating a database engine
nyc_database = create_engine('sqlite:///C:/Users/biraj/Documents/Uber Analysis/nyc_database.db')

# Downloading the nyc trip data from s3 
def download_data(year, start_month, end_month, cab_type):
    
    # Download the Trip Record Data
    for month in range(start_month, end_month+1):
        urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/trip+data/"+ \
                                    cab_type+"_tripdata_"+str(year)+"-{0:0=2d}.csv".format(month), 
                                   "../data/"+cab_type+"/nyc_"+cab_type+'_'+str(year)+"_{0:0=2d}.csv".format(month))


def preprocessing(year, start_month, end_month, cab_type):

    # Considering only necessary for the analysis
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "PULocationID", "DOLocationID", "payment_type", "tip_amount", "total_amount"]

    # Reading nyc trip data
    j, chunksize = 1, 100000
    for month in range(start_month, end_month+1):
        fp = "../data/"+cab_type+"/nyc_"+cab_type+'_'+str(year)+"_{0:0=2d}.csv".format(month)
        for df in pd.read_csv(fp, chunksize=chunksize, iterator=True):
            df = df[cols]
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
            df["weekday"] = df["tpep_pickup_datetime"].dt.day_name()
            df['pickup_hour'] = df["tpep_pickup_datetime"].dt.hour
            df['dropoff_hour'] = df["tpep_dropoff_datetime"].dt.hour

            df.to_sql('nyc_'+cab_type+'_'+str(year)+'_'+str(month), nyc_database, if_exists='append')
            #del df
        
if __name__ == "__main__":

    # taking two inputs at a time 
    year = int(input("Enter a year: "))
    start_month, end_month = [int(x) for x in input("Enter a start and end month: ").split()]
    print("\n")
    print("Pick-up cab type from: `yellow`, `green`, `FHV`")
    cab_type = [x for x in input("Enter a cab type: ").split()]
   
    # Download nyc trip data from the s3 bucket
    download_data(year, start_month, end_month, cab_type)

    # Preprocessing the data
    preprocessing(year, start_month, end_month, cab_type)