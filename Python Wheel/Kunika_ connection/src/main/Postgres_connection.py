import psycopg2
from datetime import date
from read_yaml import *
import pandas as pd

class Postgres:
    def __init__(self, database_connection_instance):
        self.db_conn = database_connection_instance
        self.load_details()
        self.connect_postgres()

    def load_details(self):
        self.db_conn.load_details()

    def connect_postgres(self):
      try:
         conn = psycopg2.connect(
            dbname=self.db_conn.database,
            user=self.db_conn.username,
            password=self.db_conn.password,
            host=self.db_conn.host,
            port=self.db_conn.port)
         cursor = conn.cursor()
      except psycopg2.Error as e:
                print("Error connecting to Database:", e)  
      finally:
         print("Connected to Postgres")
                    
      try:
        if(self.db_conn.full_load == 'True'): 
          schema = self.db_conn.schema
          table = self.db_conn.table_name
          insert_query = f''' Select * from "{schema}".{table}; '''
          cursor.execute(insert_query,((schema),(table,)))
          rows = cursor.fetchall()
          df = pd.DataFrame(rows) 
          if(self.db_conn.output == 'csv'):
             df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output.csv', index=False)
             print("Data exported to CSV file")
          elif(self.db_conn.output == 'json'):
             df.to_json(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output_1.json', orient='records')
             print("Data exported to JSON file")
          # for row in rows:
          #   print(row)
          conn.commit()
        else:
          if(self.db_conn.start != None and self.db_conn.end != None):
             schema = self.db_conn.schema
             table = self.db_conn.table_name
             start = self.db_conn.start
             end = self.db_conn.end
             insert_query = f''' Select * from "{schema}".{table} where update_date between '{start}' and '{end}'; '''
             cursor.execute(insert_query,((schema),(table),(start),(end,)))
             rows = cursor.fetchall()
             df = pd.DataFrame(rows) 
             if(self.db_conn.output == 'csv'):
               df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output.csv', index=False)
               print("Data exported to CSV file")
             elif(self.db_conn.output == 'json'):
               df.to_json(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output_1.json', orient='records')
               print("Data exported to JSON file")
            #  for row in rows:
            #     print(row)
             conn.commit()

          elif(self.db_conn.start != None):
             schema = self.db_conn.schema
             table = self.db_conn.table_name
             start = self.db_conn.start
             end = date.today()
             insert_query = f''' Select * from "{schema}".{table} where update_date between '{start}' and '{end}'; '''
             cursor.execute(insert_query,((schema),(table),(start),(end,)))
             rows = cursor.fetchall()
             df = pd.DataFrame(rows) 
             if(self.db_conn.output == 'csv'):
               df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output.csv', index=False)
               print("Data exported to CSV file")
             elif(self.db_conn.output == 'json'):
               df.to_json(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output_1.json', orient='records')
               print("Data exported to JSON file")
            #  for row in rows:
            #     print(row)
             conn.commit()              
             
          elif(self.db_conn.frequency == 'daily'):
            schema = self.db_conn.schema
            table = self.db_conn.table_name
            today = date.today()
            insert_query = f''' Select * from "{schema}".{table} where update_date='{today}'; '''
            cursor.execute(insert_query,((schema),(table),(today,)))
            rows = cursor.fetchall()
            df = pd.DataFrame(rows) 
            if(self.db_conn.output == 'csv'):
               df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output.csv', index=False)
               print("Data exported to CSV file")
            elif(self.db_conn.output == 'json'):
               df.to_json(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output_1.json', orient='records')
               print("Data exported to JSON file")
            # for row in rows:
            #   print(row)
            conn.commit()

          elif(self.db_conn.frequency == 'monthly'):
            schema = self.db_conn.schema
            table = self.db_conn.table_name
            today = date.today()
            if today.month != 1:
              old_month = today.month-1
              prev_date = today.replace(month=old_month)
            else:
               old_year = today.year-1
               prev_date = today.replace(month=12, year=old_year)
            insert_query = f''' Select * from "{schema}".{table} where update_date between '{prev_date}' and '{today}'; '''
            cursor.execute(insert_query,((schema),(table),(prev_date),(today,)))
            rows = cursor.fetchall()
            df = pd.DataFrame(rows) 
            if(self.db_conn.output == 'csv'):
               df.to_csv(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output.csv', index=False)
               print("Data exported to CSV file")
            elif(self.db_conn.output == 'json'):
               df.to_json(r'C:\Users\kunika\Python Wheel\Kunika_ connection\src\Output_Postgres\Output_1.json', orient='records')
               print("Data exported to JSON file")
            # for row in rows:
            #   print(row)
            conn.commit()            

        cursor.close()
        conn.close()  
      except psycopg2.Error as e:
                print("Error connecting to Table:", e)