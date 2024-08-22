import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET


log_file = 'log_file.txt'
target_file='transformed_data.csv'

# process to extract data
def extract():
    extracted_data = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel']) # create an empty data frame to hold the extracted data

    # extract csv files data
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)

    # extract json files data
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    # extract xml files data
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)

    return extracted_data

def extract_from_csv(file_to_extract):
    df = pd.read_csv(file_to_extract)
    return df

def extract_from_json(file_to_extract):
    df = pd.read_json(file_to_extract, lines = True)
    return df

def extract_from_xml(file_to_extract):
    df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file_to_extract)
    root = tree.getroot()
    
    for car in root:
        car_model = car.find('car_model').text
        year_of_manufacture = int(car.find('year_of_manufacture').text)
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        df = pd.concat([df, pd.DataFrame([{'car_model':car_model, 'year_of_manufacture': year_of_manufacture, 'price':price, 'fuel':fuel}])], ignore_index = True)
    
    return df


# process to transform the data
def transform(data):

    '''
    Transform price to be rounded only to 2 decimal places
    '''
    data['price'] = round(data.price, 2)

    return data

# process to load the data
def load(targetfile, transformed_data):
    transformed_data.to_csv(targetfile)

# function to log the progress
def log_progress(message):
    timestamp_format = '%Y-%m-%D-%H:%M:%S'
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message} \n")


# Log the initialization of the ETL process 
log_progress("ETL Job Started") 

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract()

# Log the beginning of the Transformation process
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data)
print("Transformed Data") 
print(transformed_data) 

# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load(target_file,transformed_data) 

# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 







