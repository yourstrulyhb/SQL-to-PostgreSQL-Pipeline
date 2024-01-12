# Extract and Load
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import os


uid = os.environ['ETL_PGUID']
pwd = os.environ['ETL_PGPASS']

# SQL database details
driver = "{SQL Server Native Client 11.0}"
sql_server = 'YATCH\SQLEXPRESS' # YATCH (?) localhost (?)
postgres_server = 'localhost'
database = "AdventureWorksDW2019" 
PORT=1433

def extract():
   try:
      connection_string = 'DRIVER=' + driver + ';SERVER=' + sql_server + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd
      connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
      src_engine = create_engine(connection_url)
      src_conn = src_engine.connect()
      # execute query
      query = """ select  t.name as table_name
      from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """
      src_tables = pd.read_sql_query(query, src_conn).to_dict()['table_name']

      for id in src_tables:
         table_name = src_tables[id]
         df = pd.read_sql_query(f'SELECT * FROM {table_name}', src_conn)
         load(df, table_name)
   
   except Exception as e:
      print("Data extract error: " + str(e))


# Load data to Postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{postgres_server}:5432/AdventureWorks') 
        print(f'Importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, chunksize=100000)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))


try:
    #call extract function
    extract()

except Exception as e:
    print("Error while extracting data: " + str(e))