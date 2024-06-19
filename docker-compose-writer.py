import sys
import uuid

def generate_docker_compose(num_clients=1):
    header = """version: '3'
networks:
  default:
    name: tp1-distribuidos_default
    external: true

services:
"""
    services = []
    for i in range(num_clients):
        client_id = str(uuid.uuid4())
        service_name = f'client{i + 1}'
        service = f"""
  {service_name}:
    image: client:latest
    entrypoint: python3 /app/main.py
    volumes:
      - ./config/client.ini:/app/config.ini
      - ./lib:/app/lib
      - ./data/{client_id}:/app/data
    environment:
      - CLIENT_ID={client_id}
      - OUTPUT_DIR=/app/data/results
    networks:
      - default
"""
        services.append(service)

    content = header + "".join(services)

    with open('docker-compose-client.yaml', 'w') as file:
        file.write(content)

if __name__ == '__main__':
    num_clients = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    generate_docker_compose(num_clients)
    print(f"docker-compose.yml generated successfully for {num_clients} clients.")
