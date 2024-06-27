#!/bin/bash

containers_exclude=("client" "rabbitmq")
list_containers() {
    docker ps -a --format '{{.Names}}' | grep -v -E "$(IFS=\|; echo "${containers_exclude[*]}")"
}

main() {
    KILL_INTERVAL=${1:-30}

    while true; do
        containers=($(list_containers))

        if [ ${#containers[@]} -gt 0 ]; then
            container_to_kill=${containers[$RANDOM % ${#containers[@]}]}
            docker kill "$container_to_kill"
            echo "Contenedor $container_to_kill ha sido detenido."
        else
            echo "No hay contenedores disponibles para detener."
        fi

        sleep "$KILL_INTERVAL"
    done
}

main "$@"
