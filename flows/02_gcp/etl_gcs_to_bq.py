from pathlib import Path
import pandas as pd
from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_from_gcs(color:str,year:int,month:int)->Path:
    gcs_path=f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block=GcsBucket.load("zoomcamp-gcsbucket")
    gcs_block.get_directory(from_path=gcs_path,local_path=f"../data")
    return Path(f"../data/{gcs_path}")

@task()
def transform(path:Path)->pd.DataFrame:
    df=pd.read_parquet(path)
    print(f"pre: missing passenger count:{df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post: missing passenger count:{df['passenger_count'].isna().sum()}")
    return df
@task()
def write_bq(df:pd.DataFrame)->None:
    from prefect_gcp import GcpCredentials
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")
    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="certain-grammar-396219",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq():
    color="yellow"
    year=2021
    month=1
    path=extract_from_gcs(color,year,month)
    df=transform(path)
    write_bq(df)


if __name__=="__main__":
    etl_gcs_to_bq()
