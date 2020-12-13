#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    file=open(saveLocation1,'w')
    for entry in collection.find():
        if entry['city'].lower()==cityToSearch.lower():
            file.write(entry['name'].upper()+'$'+entry['full_address'].upper()+'$'+entry['city'].upper()+'$'+entry['state'].upper()+'\n')
    file.close()
            

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    file=open(saveLocation2,'w')
    for entry in collection.find():
        if Distance(entry['latitude'],entry['longitude'],myLocation)<=maxDistance:
            for category in categoriesToSearch:
                if category in entry['categories']:
                    file.write(entry['name'].upper()+'\n')
                    break;                   
    file.close()
    
def Distance(lat1, lon1,myLocation):
    lat2=float(myLocation[0])
    lon2=float(myLocation[1])
    R = 3959; # miles
    φ1 = math.radians(lat1)
    φ2 = math.radians(lat2)
    Δφ = math.radians(lat2-lat1)
    Δλ = math.radians(lon2-lon1)
    a = math.sin(Δφ/2) * math.sin(Δφ/2) +math.cos(φ1) * math.cos(φ2) *math.sin(Δλ/2) * math.sin(Δλ/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d
    

