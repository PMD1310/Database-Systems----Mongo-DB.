import mysql.connector
import json
import pymongo
from bson import json_util
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client["finaltable"]

cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1',
                              database='player-stadium')
cursor = cnx.cursor()

array = []
print("Getting required data from MySQL")
print("--------------------------------")

query = "SELECT * FROM `finalview` ORDER BY Team"
cursor.execute(query)
print "pramod"

data = cursor.fetchall()
print data

for row in data:
   subobj = {}
   subobj['TEAM'] = row[0]
   subobj['MATCHDATE'] = row[1]
   subobj['CITY'] = row[2]
   subobj['STADIUMNAME'] = row[3]
   subobj['TEAMNAME1'] = row[4]
   subobj['TEAMSCORE1'] = row[5]
   subobj['TEAMNAME2'] = row[6]
   subobj['TEAMSCORE2'] = row[7]
   array.append(subobj)
my_json_string = json.dumps(array)
print my_json_string

cursor.close()
cnx.close()

print("Importing into Mongo DB")
print("--------------------------------")
result = db.finaltable.insert_many(array)
print(result.inserted_ids)

