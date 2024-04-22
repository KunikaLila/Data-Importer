from read_yaml import *
from Postgres_connection import Postgres
from Mongodb_connection import MongoDB

if __name__ == "__main__":
    print("Enter YAML File path: ")
   #  config_path = r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Input\config.yaml'
    config_path = input()
    details = DatabaseConnection(config_path)
    dbname = details.load_details()
    print("YAML File Successfully Imported")
    if(dbname == 'postgres'):
      connection = Postgres(details)

    elif(dbname == 'mongodb') :
       connection = MongoDB(details)

    else:
       print("database_type not defined")   
