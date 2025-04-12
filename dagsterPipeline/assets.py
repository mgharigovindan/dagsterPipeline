from dagster import asset, get_dagster_logger
from pymongo import MongoClient
import json
from pandas import json_normalize
import pandas as pd
from sqlalchemy import create_engine

@asset
def load_json_to_mongodb():
    client = MongoClient("mongodb://localhost:27017")
    db = client["EV"]
    collection = db["evData"]

    with open("C:\\Users\\harig\\APDV\\dagsterPipeline\\data\\updated_AQ.json") as f:
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
    return df_data

@asset(deps=[prep_jsonData])
def write_jsonToPostgres(prep_jsonData):
    engine = create_engine('postgresql://postgres:root@localhost:5432/APDV')
    prep_jsonData.to_sql('AirQuality', engine,if_exists='replace')