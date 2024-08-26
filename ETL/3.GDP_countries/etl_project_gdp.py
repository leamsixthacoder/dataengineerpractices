# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup as bp
import pandas as pd 
import numpy as np 
from datetime import datetime
import sqlite3

log_file = 'etl_project_log.txt'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = bp(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and "—" not in col[2]:
                data_dict = {"Country": col[0].a.contents[0], "GDP_USD_millions": col[2].contents[0]}
                df_gdp_data = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df_gdp_data], ignore_index = True)
    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    gdp = df['GDP_USD_millions'].tolist()
    gdp = [float("".join(x.split(','))) for x in gdp]
    gdp = [np.round(x/1000,2) for x in gdp]
    df['GDP_USD_millions'] = gdp
    df = df.rename(columns = {'GDP_USD_millions':'GDP_USD_billions'})
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%m-%D-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp}, {message} \n")




    
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project.'''


log_progress('Preliminaries complete')

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = pd.DataFrame(columns=['Country', 'GDP_USD_millions'])
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
query_statement = f'SELECT * from {table_name} WHERE GDP_USD_billions >= 100'

log_progress('Initiating ETL process...')

df = extract(url, table_attribs)

log_progress('Data extraction complete')

log_progress('Initiating Transformation process...')

df_transformed = transform(df)

log_progress('Data transformation complete')

log_progress('Initiating loading process...')

load_to_csv(df_transformed, csv_path)

log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated')

sql_connection = sqlite3.connect(db_name)
load_to_db(df_transformed, sql_connection, table_name)

log_progress('Data loaded to Database as table')

log_progress('Running the query...')

run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()





