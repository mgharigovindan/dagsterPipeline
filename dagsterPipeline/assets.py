import os
import time
import json
import requests
import pandas as pd

from pymongo import MongoClient
from pandas import json_normalize
from sqlalchemy import create_engine
from dagster import asset, get_dagster_logger

output_dir = os.path.join(os.getcwd(), "data")
os.makedirs(output_dir, exist_ok=True)

@asset
def load_json_to_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    db = client["EV"]
    collection = db["evData"]
    json_path = os.path.join(output_dir,"updated_EV_Population.json")

    with open(json_path) as f:
        data = json.load(f)

    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)


    return collection.name

@asset(deps=[load_json_to_mongodb])
def prep_jsonData():
    logger = get_dagster_logger()
    client = MongoClient("mongodb://localhost:27017/")
    db = client["EV"]
    collection = db["evData"]
    doc = collection.find_one()

    #Creating dataframe
    meta_list = doc['meta']
    data_list = doc['data']
    df_meta = pd.DataFrame(meta_list)
    df_data = pd.DataFrame(data_list)

    # Preparing dataframe
    cols = json_normalize(df_meta.view.columns)
    final_col = cols[8:].name
    df_data.drop(df_data.columns[:8],inplace=True,axis=1)
    df_data.columns = final_col
    logger.info(f"Prepared DataFrame:\n{df_data.head()}")
    df_data.to_csv(os.path.join(output_dir,'AQ.csv'),index=False)
    return df_data

@asset(deps=[prep_jsonData])
def write_jsonToPostgres(prep_jsonData):
    engine = create_engine('postgresql://postgres:root@localhost:5432/APDV')
    prep_jsonData.to_sql('evpopulation', engine,if_exists='replace')

@asset
def read_Api():
    params = "88101,42602,42101,44201,42401"
    years = list(range(2017, 2025))
    states = [f"{i:02d}" for i in range(1, 57)]

# Store all results
    all_results = []

    for state in states:
        for year in years:
            bdate = f"{year}0101"
            edate = f"{year}1231"
            url = (
            f"https://aqs.epa.gov/data/api/qaAnnualPerformanceEvaluations/byState"
            f"?email=ezkel0x0@gmail.com&key=khakihawk62&param={params}&bdate={bdate}&edate={edate}&state={state}"
        )
        
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if 'Data' in data and data['Data']:
                all_results.extend(data['Data'])
                print(f"Data Fetch for state {state}, year {year} complete.")
            else:
                print(f"Data for state {state}, year {year} not available.")
        else:
            print(f"Error {response.status_code} for state {state}, year {year}")
        
        time.sleep(1)

    df = pd.DataFrame(all_results)

    with open(os.path.join(output_dir,"epa_combined_data.json"), "w") as f:
        json.dump(all_results, f, indent=2)

    df.to_csv(os.path.join(output_dir,"epa_combined_data.csv"), index=False)
    return df

@asset(deps=[read_Api])
def write_csv_ToPostgres():
    engine = create_engine('postgresql://postgres:root@localhost:5432/APDV')
    csvI = pd.read_csv(os.path.join(output_dir,"epa_combined_data.csv"))
    csvII = pd.read_csv(os.path.join(output_dir,"ev_charging_patterns.csv"))
    csvI.to_sql('airquality', engine,if_exists='replace')
    csvII.to_sql('evchargingpatterns', engine,if_exists='replace')