import psycopg2 
from flask import Flask, request
from werkzeug.exceptions import BadRequest
import json 
import re # regular expression to handle non-standard addresss input
import datetime #  process datatime 
import logging # for error message reporting 

# error message
logger = logging.getLogger()



app = Flask(__name__)

# custom function to read .json input 
def custom_get_json():
    try:
        return request.get_json()
    
    except BadRequest:
        json_raw = request.get_data(as_text=True)
        
        # Remove integer with leading zeros: e.g. 'plz': 07115
        query_dict = re.sub(r'\b0+(\d+)', r'\1', json_raw)
        
        # Convert the query dictionary to a JSON string
        json_data = json.loads(query_dict)
        
        return json_data


connection = psycopg2.connect(user="zhoubin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="hks")


ADDRESS_MATCH = """SELECT str,hnr,postonm,latitude,longitude FROM hauskds WHERE 
	daitch_mokotoff(%s) && daitch_mokotoff(str) AND
	hnr = (%s) AND 
 	(postplz = (%s) OR
  	daitch_mokotoff(%s) && daitch_mokotoff(postonm));"""



@app.get("/geocoder/")
def get_coords():
    data = custom_get_json()

    street = data.get("str")  # if not found, return None.  str should be reserved as data type keywore. 
    
    hnr = data.get("hnr",0) # if HausNr. not defined, return 0, centroid of the street returned.
    # if hnr is read as an string, remove adz, and convert to integer!
    if isinstance(hnr,str):
        hnr = re.sub("[^0-9,-]", "", hnr).split("-") # for entry such as "12-14","12a"
        hnr = list(map(int,hnr)) # or assign the house number before "-"::  int(hnr[0])
        hnr = int(sum(hnr)/len(hnr)) # use the central house number to represent hnr
        
    plz = data.get("plz") 
    ort = data.get("ort")
    stime = data.get("time")

    if stime: # if stime is not None
        try:
            time_stamp = datetime.datetime.strptime(stime,"%Y-%m-%d")  # 2022-10-01
            t_info = stime
            print("Datetime Read Properly!")        
        except Exception as e:
            logger.error('Error: %s', e)
            t_info = "Incorrect time format"
    else:
        print("No Datetime Entry!")
        t_info = "Missing time input"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(ADDRESS_MATCH,(street,hnr,plz,ort,))
            location = cursor.fetchone()
            
    if location is not None:  # return value should be json-like format, dict     
        return json.dumps((*location,t_info)), 201
    else:
        return {'Error':'Address Not Found'}, 404





