from dagster import Definitions, load_assets_from_modules, define_asset_job
from dagsterPipeline import assets  # adjust this import if the structure differs

all_assets = load_assets_from_modules([assets])

pipeline = define_asset_job(
    name="Full_Data_Pipeline",
    selection=[
        "load_json_to_mongodb",
        "prep_jsonData",
        "write_jsonToPostgres",
        "write_csv_ToPostgres"
    ]
)

defs = Definitions(
    assets=all_assets,
    jobs=[pipeline]
)
