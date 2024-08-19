from fastapi import FastAPI, Query
from pydantic import BaseModel
import mysql.connector
from typing import List

app = FastAPI()

# Define the database connection parameters
DB_CONFIG = {
    'user': 'root',
    'password': '@dmintce',
    'host': 'localhost',
    'database': 'workshop',
}
@app.get("/getcheckin")
def get_checkin(
    input1: str,
    input2: float,
    input3: float
):
    # Connect to the database
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor(dictionary=True)

    # SQL query
    query = """
    SELECT ship_point, province 
    FROM shipment 
    WHERE car_license = %s 
 
    """
    cursor.execute(query, (input1, input2, input3))

    # Fetch results
    results = cursor.fetchall()

    # Close the connection
    cursor.close()
    connection.close()

    return results