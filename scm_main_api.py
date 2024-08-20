#fastapi2postman --app your_fastapi_app:app --output your_collection.json
#fastapi dev scm_main_api.py // don't specific IP address
#fastapi dev scm_main_api.py --host 192.168.112.186 --port 8000  // specific IP address

from datetime import datetime
from typing import Union
from fastapi import FastAPI
from fastapi import File, UploadFile
from PIL import Image
import pytesseract
import os
import time
import re


import mysql.connector
from mysql.connector import Error
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from geopy.distance import great_circle
from typing import List

app = FastAPI()
sql_execute_success = "Update completed."
max_warehouse_distance_km = 40
max_bill_distance_km      = 100
# Define the database connection parameters
DB_CONFIG = {
    'user': 'root',
    'password': '@dmintce',
    'host': 'localhost',
    'database': 'workshop',
}
def sql_query(sql_str: str):
    message = ''
    try:
        connection = mysql.connector.connect(**DB_CONFIG)

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)            
            # Query to get all warehouses             
            cursor.execute(sql_str)
            data = cursor.fetchall() 
            return data

    except Error as e:
        # print(f"404 Error: {e}")
        message = f"404 Error: {e}"
        return message

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def sql_execute(sql_str: str) -> int:
    rowcount = 0
    try:
        connection = mysql.connector.connect(**DB_CONFIG)

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)            
            # Query to get all warehouses             
            cursor.execute(sql_str)
            rowcount = cursor.rowcount
            connection.commit()
            # print(rowcount)
            return rowcount
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database error: {err}")

    except Error as e: 
        return  f"500 Database Error: {e}"

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()      
            
                
# Helper function to calculate distance
def calculate_distance(lat1, lon1, lat2, lon2) -> float:
    return round(great_circle((lat1, lon1), (lat2, lon2)).km, 2)

# Helper function to get shipment list if distance between warehouse site and agent's geolocation 
# is within max_warehouse_warehouse_distance_km
def get_shipment(lat, lon, max_warehouse_distance_km, car_license):
    message = ''
    try:
        connection = mysql.connector.connect(**DB_CONFIG)

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            
            # Query to get all warehouses
            query = """
            SELECT site, latitude, longitude  
            FROM warehouse
            """
            cursor.execute(query)
            warehouses = cursor.fetchall()
            # print(warehouse)
           
            # Filter warehouses based on distance
            warehouses_site = []
            for warehouse in warehouses:
                dist = calculate_distance(lat, lon, warehouse['latitude'], warehouse['longitude'])
                print(dist)
                if dist <= max_warehouse_distance_km:
                    warehouses_site.append(warehouse['site'])
            
            result = ' '
            result = ', '.join(f"'{item}'" for item in  warehouses_site  )
            
            # Create the SQL query text
            shipment_query = ''
            print(result)
            if result != ' ':
                shipment_query = f"""SELECT s.ship_point, s.shipid, p.province 
                FROM shipment  s 
                inner join province p
                on s.postcode = p.postcode
                WHERE car_license = '{car_license}'
                  and load_stat = '' and ship_point in ({result})"""
                print(shipment_query)
                
            # Execute the shipment query
            cursor.execute(shipment_query)
            shipment = cursor.fetchall()
            return shipment

    except Error as e:
        print(f"404 Error: {e}")
        message = f"404 Error: {e}"
        return message

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Get Shipment list where the distance between the geolocation of the agent and the warehouse site within max_warehouse_distance_km Kilometer 
@app.get("/pod_checkin")
def checkin(
    car_license: str,
    latitude: float,
    longitude: float    
    ):
    # Fetch shipment data
    result = get_shipment(latitude, longitude, max_warehouse_distance_km, car_license)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

# Get Shipment list where the distance between the geolocation of the agent and the warehouse site within max_warehouse_distance_km Kilometer 
#in process
######################################
@app.post("/pod_checkin")
def checkin(
    car_license: str    
    ):
    # Fetch shipment data
    result = get_shipment(car_license)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}


# Get Shipment list where the pick_status is 'Picking‘ OR ‘Picked’
@app.get("/pod_picked")
def picked(
    car_license: str 
    ):
    # Fetch shipment data
    str_sql = f""" SELECT s.ship_point,  s.shipid, s.dock_no, p.province
              FROM shipment s 
              inner join province p 
              on s.postcode = p.postcode
             WHERE car_license = '{car_license}'
               AND load_stat = 'Check In'
               AND pick_stat in ('Picking','Picked') 
             """
    print(str_sql)
    result = sql_query(str_sql)
    
    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}
 
 
@app.get("/pod_warehouse")
def Parameters(
    
    ):
    # Fetch warehouse data
    str_sql = "select * from warehouse"
    
    result = sql_query(str_sql)
    print(result)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

@app.get("/pod_loaded")
def Loaded(
     car_license: str   
    ):
    # Fetch shipment data
    str_sql = f"""SELECT s.ship_point,  s.shipid, s.dock_no, p.province
              FROM shipment s 
              inner join province p 
              on s.postcode = p.postcode
             WHERE car_license = '{car_license}'
                AND load_stat = 'Check In'
               	AND pick_stat = 'Picked'
             """
    print(str_sql)
    result = sql_query(str_sql)
    print(result)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}


@app.post("/pod_loaded")
def Loaded(
     car_license: str   
    ):
    # Update delivery header(doh) 
    # load_stat = ‘In-Transit’ 
    # vddate = system date time
    str_sql = f""" update shipment set load_stat = 'In-Transit',  vddate = NOW()
                 WHERE car_license = '{car_license}'                 
                   AND load_stat = 'Check In'
                   AND pick_stat = 'Picked'
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}
        
@app.post("/pod_loaded")
def Loaded(
     car_license: str   
    ):
    # Update delivery header(doh) 
    # load_stat = ‘In-Transit’ 
    # vddate = system date time
    str_sql = f""" update shipment set load_stat = 'In-Transit',  vddate = NOW()
                 WHERE car_license = '{car_license}'                 
                   AND load_stat = 'Check In'
                   AND pick_stat = 'Picked'
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}
        
 
@app.post("/pod_cfdelivery")
def Loaded(
     car_license: str   
    ):
    # Update delivery header(doh) 
    # load_stat = ‘In-Transit’ 
    # vddate = system date time
    str_sql = f""" update shipment set load_stat = 'In-Transit',  vddate = NOW()
                 WHERE car_license = '{car_license}'                 
                   AND load_stat = 'Check In'
                   AND pick_stat = 'Picked'
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        return {"message": "Update successful", "rows_updated": rowcount} 
    