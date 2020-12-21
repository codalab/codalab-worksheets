import codalab.worker.docker_utils as docker_utils
import docker

client = docker.from_env(timeout=1000)
con_id = 'cf336b16097e'
container = client.containers.get(con_id)
print('stats from api : ')
print(container.stats(stream=False))
print(docker_utils.get_container_stats(container))
