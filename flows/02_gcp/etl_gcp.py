from pathlib import Path
import pandas as pd
from prefect import task,flow
from prefect.filesystems import GitHub
from prefect_gcp.cloud_storage import GcsBucket

'''source /mnt/d/miniconda/bin/activate
conda activate week2orchestration

'''
@task(log_prints=True)
def fetch(dataset_url:str)->pd.DataFrame:
    '''read taxi data from web into dataframe'''
    df=pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame)->pd.DataFrame:
    df["lpep_pickup_datetime"]=pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"]=pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df :pd.DataFrame,color:str,dataset_file:str) ->Path:
    """write DF locally as parquet file"""
    path=Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    print(f"local path:{path}")
    return path
@task()
def write_gcs(path:Path)->None:
    """will write the parquet file to GCS bucket"""
    gcs_block=GcsBucket.load("zoomcamp-gcsbucket")# bucketname
    gcs_block.upload_from_path(from_path=f"{path}",to_path=path)
    return



@flow(name="MainFlow",log_prints=True)
def etl_web_gcs(color:str,month:int,year:int) ->None:
    '''The main ETL function'''   
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    print(f"{dataset_file}")
    #dataset_url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2020-11.parquet"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch(dataset_url)
    df_clean=clean(df)
    
    path=write_local(df,color,dataset_file)
    print(f"length:{len(df)}")
    write_gcs(path)


if __name__=='__main__':
    color ="green"
    year = 2020
    month =11
    etl_web_gcs(color,month,year)

