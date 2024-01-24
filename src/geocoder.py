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

# custom function to read .json input, when the JSON file contains non-standard format
#     e.g., PLZ with leading 0. 01687 is not json-compatible format.  

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

##  VERSION 1.0
# ADDRESS_MATCH = """SELECT str,hnr,postonm,latitude,longitude FROM hauskds WHERE 
# 	daitch_mokotoff(%s) && daitch_mokotoff(str) AND
# 	hnr = (%s) AND 
#  	(postplz = (%s) OR
#   	daitch_mokotoff(%s) && daitch_mokotoff(postonm));"""

## VERSION 2.0
# ADDRESS_MATCH = """SELECT str,hnr,postonm,latitude,longitude FROM hauskds WHERE 
# 	daitch_mokotoff(%s) && daitch_mokotoff(str) AND
# 	hnr = (%s) AND 
#  	(postplz = (%s) OR
#   	 (%s) ~* postonm);"""


ADDRESS_MATCH = """select * from 
address_match(%s,%s,%s,%s,%s);"""  # street, house_number, add_address, city, plz, lat, lon, logger

# regular expression to check if a string ended with number(s), or LIKE ~ 12 - 15, 12c 
# Street names with numbers, e.g., "Straße der 17. Juni" do not fall into this case.
re_str_hnr = re.compile(r'([0-9]{0,5}\W{0,3}[0-9]+)([a-zA-Z]{0,1})$')

# Function to convert house number (hnr) to integer
#  e.g. "101" -- 101, "12c" -- 12, "12-14c" -> 13.
def hnr_string_to_int(hnr_string):
    hnr_string = re.sub("[^0-9,-]", "", hnr_string).split("-") # for entry such as "12-14","12a"
    hnr_int = list(map(int,hnr_string)) # or assign the house number before "-"::  int(hnr[0])
    hnr_int= int(sum(hnr_int)/len(hnr_int)) 
    return hnr_int


@app.get("/geocoder/")
def get_coords():
    data = custom_get_json()

    street = data.get('str','')  # if not found nor undefined, return ''. 

    # regular expression to replace abbre.
    street = re.sub("[sS]tr[.]{0,1}", "straße", street) # replace 'str.' to 'straße'
    
    # IF street name is correctly given, assign hnr_derived to 0.
    # hnr_derived will be used to assign hnr, if hnr is not given in JSON file.
    hnr_derived = 0; 
    adz_derived = '';
    
    # if street name ENDED with numbers, it is highly probable that house number is included.
    # update the default hnr_derived 
    if re_str_hnr.search(street): # if True (street name ended with numbers)
        hnr_string = re_str_hnr.search(street) # return the matched string(s) 
        street = street.replace(hnr_string.group(0),'') # update the read street name
        hnr_derived = hnr_string_to_int(hnr_string.group(1)) # overwrite the default hnr_derived
        if hnr_string.group(2) != '':
            adz_derived = hnr_string.group(2) # separate adz from string.
        
    # read house number (hnr); IF not defined, return 0, centroid of the street returned
    hnr = data.get("hnr",hnr_derived)

    if hnr is None or not hnr:
        hnr = hnr_derived
    
    # if hnr is read as an string, remove adz, and convert to integer!
    if isinstance(hnr,str): # if hnr is a string. # str is data type. DO NOT USE IT AS VARIABLE NAME !
        hnr = hnr_string_to_int(hnr)
    
    # get ZIP code, if not available, return None   
    plz = data.get("plz",0)

    # get adressezusatz (adz)
    adz = data.get("adz", adz_derived) # if unavailable, using previously derived value - ''

    # get city name (ort)
    ort = data.get("ort")

    # get time stampe. 
    stime = data.get("time")
    
    ## Handle time input::
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
            cursor.execute(ADDRESS_MATCH,(street,hnr,adz,ort,plz,))
            location = cursor.fetchone()
                
                
    if location is not None:  # return value should be json-like format, dict     
        return json.dumps((*location,t_info)), 201
    else:
        return {'Error':f"{street} {hnr},{ort},{plz} - Not Found!"}, 404





