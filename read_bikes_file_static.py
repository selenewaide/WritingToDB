#!/usr/bin/env python3

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
if len(sys.argv) > 1:
    directory = sys.argv[1]
else:
    directory = "."

print("Reading files from ", directory)

for filename in os.listdir(directory):
    suffix_bike = "bikes.JSON"
    if not filename.endswith(suffix_bike):
        continue
        
    with open(directory + '/' + filename) as json_bike_data:
        print(filename)
        bike_data = json.load(json_bike_data)
        
        for each_station in bike_data:
            with connection.cursor() as cursor:
                get_max_timestamp = "SELECT MAX(last_update) AS max_num  FROM BikeAndWeather.StationsDynamicTestCopy WHERE station = " + str(each_station['number'])
                cursor.execute(get_max_timestamp)
                result = cursor.fetchone()
            max_timestamp = result[0]
            
            if max_timestamp is None:
                max_timestamp = 0
            max_timestamp = float(max_timestamp)
        
            if (each_station['last_update']/1000) > max_timestamp:
                
                with connection.cursor() as cursor:
                    # Create a new record
                    sql = "INSERT INTO `StationsDynamicTestCopy` (`station`, `status`, `available_bike_stands`,`available_bikes`, `last_update`) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(sql, (each_station['number'], each_station['status'], each_station['available_bike_stands'], each_station['available_bikes'],(each_station['last_update']/1000)))
                
                    # connection is not autocommit by default. So you must commit to save
                    # your changes.
                    connection.commit()
            else:
                print("Skipping station ", each_station['number'], " because ", max_timestamp, " is greater than ", each_station['last_update']/1000)
        
        
# MAX(last_update) AS max_num  FROM BikeAndWeather.StationsDynamicTestCopy WHERE station = 42      
        