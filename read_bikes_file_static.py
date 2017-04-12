#!/usr/bin/env python3

'''
Reads each bike JSON file, and writes data into the static station table.
Checks for duplicate station/timestamp combinations and ignores these.
'''

import json
import os
import sys
import pymysql
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='bikeandweather.cnkbtyr1hegq.us-east-1.rds.amazonaws.com',
                             user='admin',
                             password='Conv2017',
                             db='BikeAndWeather')

# Getting the directory name - where the JSON files are
if len(sys.argv) > 1:
    directory = sys.argv[1]
else:
    directory = "."

print("Reading files from ", directory) # ensuring we are in the right dir

# Iterating over each file in the directory
for filename in sorted(os.listdir(directory)):
    suffix_bike = "bikes.JSON"
    if not filename.endswith(suffix_bike):
        continue
    
    # get data  
    with open(directory + '/' + filename) as json_data: # looks at files in file timestamp order
        print(filename) # to give a visual check
        data_from_file = json.load(json_data)
        
        # gets the max timestamp for each station - to avoid duplicates from previous files read in
        for each_station in data_from_file:
        #=======================================================================
        #     with connection.cursor() as cursor:
        #         get_max_timestamp = "SELECT MAX(last_update) AS max_num  FROM BikeAndWeather.StationsStatic WHERE station = " + str(each_station['number'])
        #         cursor.execute(get_max_timestamp)
        #         result = cursor.fetchone()
        #     max_timestamp = result[0]
        #     
        #     if max_timestamp is None:
        #         max_timestamp = 0
        #     max_timestamp = float(max_timestamp)
        # 
        #     if (each_station['last_update']/1000) > max_timestamp: # dividing by 1000 converts time stamp into seconds, from milliseconds (conventional)
        #         
        #=======================================================================
                with connection.cursor() as cursor:
                    # Create a new record
                    sql = "INSERT INTO `StationsStatic` (`station`, `name`, `address`,`lat`, `lng`, `banking`, `bonus`, `contract_name`, `bike_stands`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (each_station['number'], each_station['name'], each_station['address'], each_station['position']['lat'],  each_station['position']['lng'], each_station['banking'],each_station['bonus'], each_station['contract_name'],each_station['bike_stands']))
        
                    # connection is not autocommit by default. Therefore commit to save
                    # changes.
                    connection.commit()
        #=======================================================================
        #     else:
        #         print("Skipping station ", each_station['number'], " because ", max_timestamp, " is greater than ", each_station['last_update']/1000)
        # 
        # 
        #=======================================================================
