from pathlib import Path
import pandas as pd
from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

'''source /mnt/d/miniconda/bin/activate
conda activate week2orchestration

'''
@task(log_prints=True)
def fetch(dataset_url:str)->pd.DataFrame:
    '''read taxi data from web into dataframe'''
    df=pd.read_csv(dataset_url)
    return df

@task()
def clean(df=pd.DataFrame)->pd.DataFrame:
    df["tpep_pickup_datetime"]=pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"]=pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_local(df :pd.DataFrame,color:str,dataset_file:str) ->Path:
    """write DF locally as parquet file"""
    path=Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path
@task()
def write_gcs(path:Path)->None:
    """will write the parquet file to GCS bucket"""
    gcs_block=GcsBucket.load("zoomcamp-gcsbucket")# bucketname
    gcs_block.upload_from_path(from_path=f"{path}",to_path=f"{path}")
    return



@flow(name="MainFlow")
def etl_web_gcs(year:int,month:int,color:str) ->None:
    '''The main ETL function'''
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch(dataset_url)
    df_clean=clean(df)
    path=write_local(df_clean,color,dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list=[1,2],year:int=2019,color:str="yellow"):
    for month in months:
        etl_web_gcs(year, month, color)
if __name__=='__main__':
    months=[1,2,3]
    color="yellow"
    year=2019
    etl_parent_flow(months,year,color)

