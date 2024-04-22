import pymongo
from pymongo import MongoClient
import logging
import pandas as pd
import json
from datetime import date
from read_yaml import *

class MongoDB:
    logging.basicConfig(level=logging.INFO)
    def __init__(self, database_connection_instance):
        self.db_conn = database_connection_instance
        self.load_details()
        self.uri = 'mongodb://127.0.0.1:27017/?appName=myApp'
        self.client = None
        self.connect()

    def load_details(self):
        self.db_conn.load_details()

    def connect(self):
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=2000)
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")
        except pymongo.errors.ConfigurationError as e:
            logging.error(f"Configuration error: {e}")
            raise ConfigurationError(f"Configuration error: {e}")
        else:
            logging.info("Successfully connected to MongoDB")
            self.fetch()

    def fetch(self):
        dbname = self.db_conn.database
        collection_name = self.db_conn.collection
        db = self.client[dbname]
        collection = db[collection_name]
        # print(collection)
        if(self.db_conn.full_load == 'True'): 
            cursor = collection.find({})
            if(self.db_conn.output == 'csv'):
               df = pd.DataFrame(cursor) 
               df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.csv', index=False)
               print("Data exported to CSV file")
            elif(self.db_conn.output == 'json'):
              data = list(cursor) 
              json_str = json.dumps(data, default=str)
              with open(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.json', 'w', encoding='utf-8') as json_file:
                  json_file.write(json_str)
              print("Data exported to JSON file")
        # for doc in cursor:
        #       print(doc)
        else:
            if(self.db_conn.start != None and self.db_conn.end != None):
                start = self.db_conn.start
                end = self.db_conn.end 
                cursor = collection.find({
                    'Date': {
                        '$gt': start,
                        '$lt': end
                        }})
                if(self.db_conn.output == 'csv'):
                   df = pd.DataFrame(cursor) 
                   df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.csv', index=False)
                   print("Data exported to CSV file")
                elif(self.db_conn.output == 'json'):
                   data = list(cursor)
                   json_str = json.dumps(data, default=str) 
                   with open(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.json', 'w', encoding='utf-8') as json_file:
                     json_file.write(json_str)
                   print("Data exported to JSON file")
                # for doc in cursor:
                #     print(doc)

            elif(self.db_conn.start != None):
                start = self.db_conn.start
                today = date.today() 
                today_iso = today.isoformat()
                cursor = collection.find({
                    'Date': {
                        '$gt': start,
                        '$lt': today_iso
                        }})
                if(self.db_conn.output == 'csv'):
                   df = pd.DataFrame(cursor) 
                   df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.csv', index=False)
                   print("Data exported to CSV file")
                elif(self.db_conn.output == 'json'):
                  data = list(cursor)
                  json_str = json.dumps(data, default=str) 
                  with open(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.json', 'w', encoding='utf-8') as json_file:
                    json_file.write(json_str)
                  print("Data exported to JSON file")
                # for doc in cursor:
                #     print(doc)

            elif(self.db_conn.frequency == 'daily'):
                today = date.today()         
                today_iso = today.isoformat()   
                cursor = collection.find({'Date': today_iso})
                if(self.db_conn.output == 'csv'):
                   df = pd.DataFrame(cursor) 
                   df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.csv', index=False)
                   print("Data exported to CSV file")
                elif(self.db_conn.output == 'json'):
                   data = list(cursor)  # Convert cursor to list
                   json_str = json.dumps(data, default=str)  # Convert list to JSON string
                   with open(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_MongoDB\Mongo_output.json', 'w', encoding='utf-8') as json_file:
                     json_file.write(json_str)
                   print("Data exported to JSON file")
                # if cursor:
                #     print(cursor)
                # else:
                #     print("No documents found for today's date")



        