import sqlalchemy_connector
import mysql_connector
import os
from datetime import datetime
import pandas as pd

# Mock functions to represent ETL steps (to be implemented)
def extract_data(file_path):    
    df = pd.read_csv(file_path,delimiter = '|')
    return df

def transform_data(df):
    #dropping blank fields and header/details row indetifier
    df = df.drop('H',axis=1).dropna(axis=1)
    # keeping latest record as per last_consulted_date per Customer
    df = df.sort_values(by='Last_Consulted_Date').drop_duplicates(subset=['Customer_Name','Last_Consulted_Date'],keep='last')
    # deriving Age
    df['Age'] = df['DOB'].map(lambda x:calculate_age(x))
    # deriving the days since last consulted date
    df['Days_Since_Last_Consulted'] = df['Last_Consulted_Date'].map(lambda x:calculate_days_since_last_consulted(x))
    # deriving the flag for (days since last consulted date)>30 days
    df['DaysSinceLastConsulted_GreaterThan30Days_Flag'] = df['Last_Consulted_Date'].map(lambda x:True if calculate_days_since_last_consulted(x)>30 else False)
    return df

def load_data_into_country_tables(final_df,engine,cur):
    for country in set(final_df["Country"].values):
        table_name = f'table_{country.lower()}'
        q = f"delete from {table_name} where customer_name in {tuple(set(final_df.Customer_Name.values))} and country='{country}';"
        cur.execute(q)
        cur.execute(f"insert into {table_name} select * from intermediate_data_table where country='{country}';")    


# Helper functions for calculation (to be implemented)
def calculate_age(dob):
    today = datetime.now()
    try: 
        dob_date = datetime.strptime(str(dob), "%Y%m%d")
    except Exception as e:
        dob_date = datetime.strptime(str(dob), "%d%m%Y")
    age = today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
    return age

def calculate_days_since_last_consulted(last_consulted):
    today = datetime.now()
    try: 
        last_date = datetime.strptime(str(last_consulted), "%Y%m%d")
    except Exception as e:
        last_date = datetime.strptime(str(last_consulted), "%d%m%Y")
    delta = today - last_date
    return delta.days

def main():
    final_df = pd.DataFrame()
    # Checking for all files ending with .csv extension 
    # and passing the files to create dataframe
    data_dir = os.path.join(os.getcwd(),'data')
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            filepath = os.path.join(data_dir,file)
            final_df = pd.concat([final_df,extract_data(filepath)])
    
    # cleaning the data and deriving new fields
    final_df = transform_data(final_df)
    
    mdb = mysql_connector.connect()
    cursor = mdb.cursor()
    cursor.execute("create database if not exists incubyte_db;")
    cursor.execute("use incubyte_db;")
    
    engine = sqlalchemy_connector.connect()
    final_df.to_sql(name='intermediate_data_table',con=engine,index=False,chunksize=15000,if_exists='replace')
        
    load_data_into_country_tables(final_df,engine,cursor)


if __name__ == "__main__":
    main()


