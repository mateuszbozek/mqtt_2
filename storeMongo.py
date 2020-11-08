from pymongo import MongoClient
from bson.objectid import ObjectId
import pymongo
import json


# client = MongoClient('mongodb+srv://mbozek:Byczku1@awsdatabase1.b1d6d.mongodb.net/mqtt?retryWrites=true&w=majority')
client = MongoClient(
    'mongodb+srv://bart6g:VqIlQIW0Oc9nh6HO@cluster0.vbxui.gcp.mongodb.net/mqtt?retryWrites=true&w=majority')

db = client.mqtt

tempSensors = db['sensors'].find({'topic':'Temperature'})


def tempHandler(jsonData):
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Temperature = json_Dict['Temperature']
    toSave = {"sensorId":  ObjectId(SensorID),
              "date": Data_and_Time, "temp": Temperature}

    db.temperature.insert_one(toSave)


def humHandler(jsonData):
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    Humidity = json_Dict['Humidity']
    toSave = {"sensorId":ObjectId(SensorID),
              "date": Data_and_Time, "humidity": Humidity}

    db.humidity.insert_one(toSave)
	
def dogFoodHandler(jsonData):
    json_Dict = json.loads(jsonData)
    SensorID = json_Dict['Sensor_ID']
    Data_and_Time = json_Dict['Date']
    DogFood = json_Dict['DogFood']
    toSave = {"sensorId": ObjectId(SensorID),
              "date": Data_and_Time, "dogFood": DogFood}

    db.dogFood.insert_one(toSave)


def sensor_Data_Handler(Topic, jsonData):

    if Topic == "Home/BedRoom/DHT22/Temperature":
        tempHandler(jsonData)
    elif Topic == "Home/BedRoom/DHT22/Humidity":
        humHandler(jsonData)
    elif Topic == "Home/BedRoom/DHT22/DogFood":
        dogFoodHandler(jsonData)
