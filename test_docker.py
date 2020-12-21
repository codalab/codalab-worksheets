import codalab.worker.docker_utils as docker_utils
import docker

client = docker.from_env(timeout=1000)
con_id = '03ea7ab137bf'
container = client.containers.get(con_id)
print('stats from api : ')
stats = container.stats(stream=False)
print(str(stats))
total = stats['cpu_stats']['cpu_usage']['total_usage']
system = stats['cpu_stats']['system_cpu_usage']
try:
    print(stats['ribima'])
except KeyError:
    pass
print(str(total / system))
print(docker_utils.get_container_stats(container))

# a = '3'
# b = '4'
# print(int(a) / int(b))
