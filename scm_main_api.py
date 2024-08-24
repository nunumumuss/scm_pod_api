#fastapi2postman --app your_fastapi_app:app --output your_collection.json
#fastapi dev scm_main_api.py // don't specific IP address
#fastapi dev scm_main_api.py --host 192.168.112.186 --port 8000  // specific IP address

from datetime import datetime
from typing import Union
from fastapi import FastAPI
from fastapi import File, UploadFile, APIRouter
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
router_pod   = APIRouter()
router_other = APIRouter()
router_user  = APIRouter()

sql_execute_success = "Update completed."
max_warehouse_distance_km = 10
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
            nearby_warehouses = []
            for warehouse in warehouses:
                dist = calculate_distance(lat, lon, warehouse['latitude'], warehouse['longitude'])
                print(dist)
                if dist <= max_warehouse_distance_km:
                    warehouses_site.append(warehouse['site'])
                else:
                   nearby_warehouses.append({
                        "site": warehouse['site'],
                        "distance": dist
                    })
            
            result = ''
            print('show result1:', result)
            result = ', '.join(f"'{item}'" for item in  warehouses_site  )
            
            # Create the SQL query text
            shipment_query = ''
            print('show result2:',result)
            if result != '':
                shipment_query = f"""SELECT s.ship_point, s.shipid, p.province ,s.dock_no
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
            else:
                print(nearby_warehouses)
                return nearby_warehouses
            

    except Error as e:
        print(f"404 Error: {e}")
        message = f"404 Error: {e}"
        return message

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Get Shipment list where the distance between the geolocation of the agent and the warehouse site within max_warehouse_distance_km Kilometer 
@router_pod.get("/checkin")
def checkin(
    car_license: str,
    latitude: float,
    longitude: float    
    ):
    # Fetch shipment data
    result = get_shipment(latitude, longitude, max_warehouse_distance_km, car_license )

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

# Get Shipment list where the distance between the geolocation of the agent and the warehouse site within max_warehouse_distance_km Kilometer 

@router_pod.post("/checkin")
def Parameters(
    ship_id: List[str]
):
    
    # á»Å§ ship_id à»ç¹ÊµÃÔ§ã¹ÃÙ»áºº ('6100155477', '6100155530')
    ship_id_str = '(' + ', '.join(f"'{id}'" for id in ship_id) + ')'
    str_sql = f"""UPDATE shipment SET load_stat = 'Check In', cidate = NOW() 
                WHERE shipid IN {ship_id_str}"""
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}
    
    
# Get Shipment list where the pick_status is 'Picking‘ OR ‘Picked’
@router_pod.get("/picked")
def Parameters(
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
 
 
@router_pod.get("/warehouse")
def Parameters(
    
    ):
    # Fetch warehouse data
    str_sql = "select * from warehouse"
    
    result = sql_query(str_sql)
    # print(result)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

@router_pod.get("/loaded")
def Parameters(
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

        
@router_pod.post("/loaded")
def Parameters(
    ship_id: List[str]
    ):
    # Update delivery header(doh) 
    # load_stat = ‘In-Transit’ 
    # vddate = system date time
    # á»Å§ ship_id à»ç¹ÊµÃÔ§ã¹ÃÙ»áºº ('6100155477', '6100155530')
    ship_id_str = '(' + ', '.join(f"'{id}'" for id in ship_id) + ')'
    str_sql = f""" update shipment set load_stat = 'In-Transit',  vddate = NOW()
                   WHERE shipid IN {ship_id_str}          
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
        
# Post -- Confirm Delivery (Êè§¢Í§ÊÓàÃç¨\äÁèÊÓàÃç¨)
@router_pod.post("/cfdelivery")
def Parameters(
     bill_no: str,
     action: str,
     remark: str,
     img_url: str
    ):
    
    # *** ÊÓ¤Ñ­ **** 
    # ¡ÓË¹´ action ãËé fix ¢éÍ¤ÇÒÁ
    # 0 = Delivered , 1 = Failed
    
    # ãªéàªç¤ action
    # if action == 'success' :
    #     action_state = 'Delivered'
    # else :
    #     action_state = 'Failed'
    
    str_sql = f""" update doh set stat = '{action}', cddate = NOW(), rem = '{remark}', Img_url = '{img_url}'
                   WHERE billno = '{bill_no}'                 
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        str_sql2 = f""" select stat from doh where shipid = (select shipid from doh where billno = '{bill_no}') """
        print('str_sql2:', str_sql2)
        result_stat = sql_query(str_sql2)
        print('result_stat', result_stat)
        if all(item["stat"] != "Preparing" for item in result_stat):
            str_sql3 = f""" update shipment set load_stat = 'Deliveried', cfdate = NOW() where shipid = (select shipid from doh where billno = '{bill_no}') """
            print(str_sql3)
            rowcount1 = sql_execute(str_sql3)
            print('update Ê¶Ò¹Ð shipment ÃÇÁ!!!')
        else :
            print('äÁè update Ê¶Ò¹Ð shipment ÃÇÁ')
    return {"message": "Update successful", "rows_updated": rowcount}
        
# Post -- Reverse Delivery (à»ÅÕèÂ¹Ê¶Ò¹ÐÊè§¢Í§ãËÁè)
@router_pod.post("/rvdelivery")
def Parameters(
     bill_no: str,
     remark : str
    ):
    
    str_sql = f""" update doh set stat = 'In-Transit', cddate = NOW(),rem = '{remark}'
                   WHERE billno = '{bill_no}'                 
               """    
    # str_sql = f""" update doh set stat = 'In-Transit', cddate = NOW() 
    #                WHERE billno = '{bill_no}'                 
    #            """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="No data found")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}
    
    
# Get -- Confirm Delivery
@router_pod.get("/cfdelivery")
def Parameters(
     bill_no: str   
    ):
         
    # Fetch cusname data
    str_sql = f"""SELECT d.billno, d.cusname, s.load_stat, d.stat as do_stat, 
                        IFNULL(s.cfdate, '') AS cfdate, IFNULL(d.cddate, '') AS cddate
                  FROM doh d
                  INNER JOIN shipment s
                  ON d.shipid = s.shipid 
                  WHERE d.billno = '{bill_no}';
                """
    print(str_sql)
    result = sql_query(str_sql)
    print(result)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

# Get -- User (Log in)
@router_user.get("/user")
def Parameters(
     username: str,
     password: str   
    ):
    
    str_sql = f"""SELECT username, email, latitude, longitude, car_license
              FROM users
              WHERE username = '{username}'
                AND password = '{password}'
              """
    print(str_sql)
    result = sql_query(str_sql)
    print(result)

    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

# Post -- Update Account (µéÍ§¡ÒÃÍÑ¾à´µ¢éÍÁÙÅãËÁè)
@router_user.post("/updateAccount")
def Parameters(
     password: str,
     car_license: str,
     new_username: str,
     new_email: str,
     new_password: str,
    ):
  
    str_sql = f""" update users set username = '{new_username}', email = '{new_email}', password = '{new_password}'
                 WHERE password = '{password}'                 
                 AND car_license = '{car_license}'
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="Incorrect old password or carlicense")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}
    
# Post -- Update Password (forget password)
@router_user.post("/updatePassword")
def Parameters(
     username: str,
     car_license: str,
     new_password: str,
    ):
  
    str_sql = f""" update users set password = '{new_password}'
                 WHERE username = '{username}'                 
                 AND car_license = '{car_license}'
               """                
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="Incorrect username or carlicense")
    else:
        return {"message": "Update successful", "rows_updated": rowcount}

# Post -- Insert New Account (ÊÁÑ¤Ã Account ãËÁè)    
 
@router_user.post("/insertNewAcc")
def Parameters(
     username: str,
     email: str,
     password: str,
     pin: int,
     latitude: int,
     longitude: int,
     car_license: str,
    ):
    
   #¡ÓË¹´ãËé account ãËÁè ·ÕèÊÃéÒ§ÁÒµéÍ§ËéÒÁÁÕ username, car_license «éÓ¡Ñº¢éÍÁÙÅ·ÕèÁÕÍÂÙè
    str_sql = f""" INSERT INTO users (username, email, password, pin, latitude, longitude, car_license)
                    SELECT '{username}', '{email}', '{password}', '{pin}', {latitude}, {longitude}, '{car_license}'
                    WHERE NOT EXISTS (SELECT 1 FROM users WHERE car_license = '{car_license}' OR username = '{username}')
               """               
    print(str_sql)
    rowcount = sql_execute(str_sql)
    print(rowcount)
    if rowcount == 0: 
        raise HTTPException(status_code=404, detail="Car license or Username already exists, insert failed")
    else:
        return {"message": "Insert successful", "rows_inserted": rowcount}


# Get Shipment list where the pick_status is 'Picking‘ OR ‘Picked’
@router_pod.get("/deliverylog")
def Parameters(
    car_license: str 
    ):
    # Fetch shipment data
    str_sql = f""" SELECT s.ship_point,  s.shipid, s.dock_no, p.province, 
                d.doid, d.cusname, d.billno, d.stat do_stat
              FROM shipment s 
              inner join province p 
              on s.postcode = p.postcode
              inner join doh d 
              on d.shipid = s.shipid
              WHERE car_license = '{car_license}' 
             """
    print(str_sql)
    result = sql_query(str_sql)
    
    if not result:
        raise HTTPException(status_code=404, detail="No data found")

    return {"results": result}

# Include the router in the FastAPI app
app.include_router(router_pod, prefix="", tags=["pod"])
app.include_router(router_other, prefix="", tags=["other"])
app.include_router(router_user, prefix="", tags=["usr"])