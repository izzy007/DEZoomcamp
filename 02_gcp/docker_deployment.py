from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.infrastructure.container import DockerContainer






docker_container_block = DockerContainer.load("zoom-container")

docker_dp=Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker-flow',
    infrastructure=docker_container_block

)

if __name__=="__main__":
    docker_dp.apply()
