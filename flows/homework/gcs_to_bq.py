from pathlib import Path
import pandas as pd
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket
from prefect import task,flow
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def fetch_from_gcs(color:str,month:int,year:int)->pd.DataFrame:
    '''fetch from gcs bucket'''
    gcs_path=(f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet")
    gcs_bucket=GcsBucket.load("zoomcamp-gcsbucket")
    gcs_bucket.get_directory(from_path=gcs_path,local_path=(f"../data"))
    localpath=Path(f"../data/{gcs_path}")
    print(f"path of files {localpath}")  
    df=pd.read_parquet(localpath)
    return df

@task(log_prints=True)
def send_to_Bq(df:pd.DataFrame)->None:
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")
    df.to_gbq(
         destination_table="dezoomcamp.yellow_trips_data_Feb_2019",
         project_id="certain-grammar-396219",
         credentials=gcp_credentials_block.get_credentials_from_service_account(),
         chunksize=500_000,
         if_exists="append"
     )




@flow(name="main flow",log_prints=True)
def etl_parent_flow(months: list=[2,3],year:int=2019,color:str="yellow"):
    total_rows=0
    for month in months:
        df=fetch_from_gcs(color,month,year)
        total_rows +=len(df)
        send_to_Bq(df)
    print(f"Number of records:{(total_rows)}")

if __name__=="__main__":
    months=[2,3]
    color="yellow"
    year=2019
    etl_parent_flow(months,year,color)
    

