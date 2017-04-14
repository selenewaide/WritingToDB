#!/usr/bin/env python3

'''
Reads each weather JSON file, and writes data into the weather table.
Checks for duplicate timestamp and ignores these.
'''

import json
import os
import sys
import pymysql
import pymysql.cursors
import datetime

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

# starting position for the hourly string checker - empty string
date_and_hour_check = ""

# Iterating over each file in the directory
for filename in sorted(os.listdir(directory)):
    suffix_bike = "weather.JSON"
    if not filename.endswith(suffix_bike):
        continue
    
    # get data  
    with open(directory + '/' + filename) as json_data: # looks at files in file timestamp order
        print(filename) # to give a visual check
        data_from_file = json.load(json_data)
        
        this_file_date_and_hour_check = datetime.datetime.fromtimestamp(
                    float(data_from_file['dt'])
                ).strftime('%Y-%m-%d %H')
        
        # gets the max timestamp in the DB - 
        # to avoid duplicates from previous files read in
        with connection.cursor() as cursor:
            get_max_timestamp = "SELECT MAX(dt) AS max_num  FROM BikeAndWeather.HourlyWeatherJSON"
            cursor.execute(get_max_timestamp)
            result = cursor.fetchone()
        max_timestamp = result[0]
        
        if max_timestamp is None:
            max_timestamp = 0
        max_timestamp = float(max_timestamp)
    
        if (data_from_file['dt']) > max_timestamp and (this_file_date_and_hour_check != date_and_hour_check): 
            
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `HourlyWeatherJSON` (`dt`, `main`, `description`,`icon`, `temp`, `json`, `date_time`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (data_from_file['dt'], data_from_file['weather'][0]['main'], data_from_file['weather'][0]['description'], data_from_file['weather'][0]['icon'], data_from_file['main']['temp'], json.dumps(data_from_file), this_file_date_and_hour_check))
    
                # connection is not autocommit by default. Therefore commit to save
                # changes.
                connection.commit()
        else:
            print("Skipping this file as the timestamp is a duplicate")
            
    date_and_hour_check = datetime.datetime.fromtimestamp(
                    float(data_from_file['dt'])
                ).strftime('%Y-%m-%d %H')
    
    
