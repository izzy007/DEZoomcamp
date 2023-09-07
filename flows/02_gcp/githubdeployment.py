from prefect.deployments import Deployment
from etl_gcp import etl_web_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoomcamp")

deployment = Deployment.build_from_flow(
     flow=etl_web_gcs,
     name="github-exercise",
     storage=storage,
     entrypoint="DEZoomcamp/flows/02_gcp/etl_gcp.py:etl_web_gcs")

if __name__ == "__main__":
    deployment.apply()