# Run all services

!!! warning    
    Running all services together can crash your machine.

## Get root directory
```
pwd
```
result:
/home/wallace/docker/data_engineering_stack

## Up Services
```
#!/bin/bash
PROJECT_ROOT="/home/fabricio/docker/data_engineering_stack"

run_docker_compose() {
    local service_path="$1"
    echo "Running docker-compose in $service_path"
    cd "$service_path" || { echo "Failed to change directory to $service_path"; exit 1; }
    docker-compose up -d
}

run_docker_compose "$PROJECT_ROOT/applications/postgres_adventureworks"
run_docker_compose "$PROJECT_ROOT/applications/minio"
run_docker_compose "$PROJECT_ROOT/applications/spark"
run_docker_compose "$PROJECT_ROOT/applications/trino"
run_docker_compose "$PROJECT_ROOT/applications/airflow"
run_docker_compose "$PROJECT_ROOT/applications/superset"
run_docker_compose "$PROJECT_ROOT/applications/open_metadata"

echo "All services started!"
```


## Down Services
```
#!/bin/bash
PROJECT_ROOT="/home/fabricio/docker/data_engineering_stack"

run_docker_compose() {
    local service_path="$1"
    echo "Running docker-compose in $service_path"
    cd "$service_path" || { echo "Failed to change directory to $service_path"; exit 1; }
    docker-compose down
}

run_docker_compose "$PROJECT_ROOT/applications/postgres_adventureworks"
run_docker_compose "$PROJECT_ROOT/applications/minio"
run_docker_compose "$PROJECT_ROOT/applications/spark"
run_docker_compose "$PROJECT_ROOT/applications/trino"
run_docker_compose "$PROJECT_ROOT/applications/airflow"
run_docker_compose "$PROJECT_ROOT/applications/open_metadata"

echo "All services stopped!"
```

## Access Services Security Mode

Jupyter

https://jupyter.nortetel.duckdns.org/

Minio

https://minio.nortetel.duckdns.org/

Open Metadata

https://openmetadata.nortetel.duckdns.org/

Spark 

https://spark.nortetel.duckdns.org/

Trino

https://trino.nortetel.duckdns.org/