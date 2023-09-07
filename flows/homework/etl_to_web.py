import os
import pandas as pd
from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
from datetime import timedelta


@task()
def fetch(weburl:str)->pd.DataFrame:
    """fetch data from url"""
    df=pd.read_csv(weburl)
    return df

@task(log_prints=True)
def write_local(df:pd.DataFrame,color:str,dataset_file:str)->Path:
    testpath=Path(f"/mnt/d/DEZoomcamp/prefect/data/{color}")
    if (os.path.exists(testpath)):
        print(f"This is the path: {testpath}")
    else:
        os.mkdir(testpath)
   
    path=Path(f"{testpath}/{dataset_file}.parquet")
    print(f"This is the path: {path}")               
    df.to_parquet(path,compression='gzip')
    return path
 


@task()
def write_to_gcs(path:Path,color:str)->None:
    """Write to GCS"""
    gcs_block= GcsBucket.load("zoomcamp-gcsbucket")
    gcs_block.upload_from_path(from_path=path, to_path=Path(f"data/{color}"))


@flow(name="main flow")
def etl_to_web(color:str,year:int,month:int):
    dataset_file=f"{color}_tripdata_{year}-{month:02}"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df=fetch(dataset_url)
    path=write_local(df,color,dataset_file)
    write_to_gcs(path,color)

@flow()
def main_flow():
    color="green"
    year=2021
    month=1
    etl_to_web(color,year,month)



if __name__=="__main__":
    main_flow()


