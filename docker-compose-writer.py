import os
import argparse

def parse_arguments():
    """ Parse command line arguments for shard and replica configurations. """
    parser = argparse.ArgumentParser(description='Generate a docker-compose-dev.yaml file with specified shard replicas for special services and individual replicas for other services.')
    parser.add_argument('--n_shards', type=int, default=1, help='Number of shards for special subdirectories')
    parser.add_argument('--replicas', nargs='+', help='Number of replicas for specific subdirectories in the format subdirectory=replicas')
    args = parser.parse_args()
    
    # Parse the replicas into a dictionary
    replica_dict = {}
    if args.replicas:
        for replica in args.replicas:
            subdirectory, count = replica.split('=')
            replica_dict[subdirectory] = int(count)
    
    return args.n_shards, replica_dict

def get_subdirectories(directorio_raiz):
    """ List all subdirectories in the given directory. """
    return [d for d in os.listdir(directorio_raiz) if os.path.isdir(os.path.join(directorio_raiz, d))]

def add_service_for_subdirectory(subdirectory, index, environment_config=""):
    """ Generate the Docker Compose configuration for a service. """
    return f"""
  {subdirectory}-{index}:
    container_name: {subdirectory}-{index}
    image: {subdirectory}:latest
    entrypoint: python3 /app/main.py
    depends_on:
      - rabbitmq
    links:
      - rabbitmq{environment_config}
"""

def generate_services(subdirectories, num_shards, replica_dict):
    """ Generate services for all subdirectories based on shard configuration and individual replica settings. """
    special_subdirectories = ['fiction_reviews_filter', 'fiction_title_barrier']  # List of special subdirectories
    content = ""
    for subdirectory in subdirectories:
        if subdirectory in special_subdirectories:
            num_replicas = num_shards
        else:
            num_replicas = replica_dict.get(subdirectory, 1)  # Default to 1 if not specified
        
        for i in range(1, num_replicas + 1):
            environment_config = get_environment_config(subdirectory, i)
            content += add_service_for_subdirectory(subdirectory, i, environment_config)
    return content

def get_environment_config(subdirectory, index):
    """ Return specific environment configuration for special subdirectories. """
    if subdirectory == "fiction_reviews_filter":
        return f"""
    environment:
      SHARD_ID: "{index}"
      RABBIT_HOSTNAME: "rabbitmq"
      SRC_QUEUE: "fiction_titles_shard{index}_queue"
      SRC_EXCHANGE: "fiction_titles_barrier_exchange"
      SRC_ROUTING_KEY: "fiction_titles_shard{index}"
      DST_EXCHANGE: "fiction_rev_exchange"
      TMP_QUEUES_PREFIX: "reviews_shard{index}"
"""
    elif subdirectory == "fiction_title_barrier":
        return f"""
    environment:
      SHARD_ID: "{index}"
      DST_ROUTING_KEY: "fiction_titles_shard{index}"
"""
    return ""

def generate_initial_content():
    """ Generates the initial static content for the docker-compose file. """
    return """
version: '3.8'
services:
  rabbitmq:
    image: rabbitmq:latest
    ports:
      - 15672:15672
  client:
    image: client:latest
    entrypoint: python3 /app/main.py
    volumes:
      - ./config/client.ini:/app/config.ini
      - ./lib:/app/lib
      - ./data:/app/data
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
  gateway:
    image: gateway:latest
    entrypoint: python3 /app/main.py
    depends_on:
      - rabbitmq
    links:
      - rabbitmq
"""

def main():
    directorio_raiz = './workers'
    num_shards, replica_dict = parse_arguments()
    subdirectories = get_subdirectories(directorio_raiz)
    services_content = generate_services(subdirectories, num_shards, replica_dict)
    docker_compose_content = generate_initial_content() + services_content
    docker_compose_path = os.path.join('docker-compose-dev.yaml')
    with open(docker_compose_path, 'w') as file:
        file.write(docker_compose_content)

if __name__ == "__main__":
    main()
