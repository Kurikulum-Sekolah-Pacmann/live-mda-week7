#!/bin/bash

# Start all airflow services
docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/airflow/docker-compose.yml up --build --detach --force-recreate

# Import airflow variables and connections
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml
docker exec -it airflow-webserver airflow variables import -a overwrite /init/variables_and_connections/airflow_variables_init.json

# Start data typesense services
docker compose -f ./setup/typesense/docker-compose.yml down -v
docker compose -f setup/typesense/docker-compose.yml up --build --detach --force-recreate

# Connect typesense to airflow network, Just in case it is not connected
docker network connect airflow-networks typesense

# Start data sources services
docker compose -f ./setup/sources/docker-compose.yml down -v
docker compose -f ./setup/sources/docker-compose.yml up --build --detach --force-recreate

# Start data warehouse services
docker compose -f ./setup/warehouse/docker-compose.yml down -v
docker compose -f setup/warehouse/docker-compose.yml up --build --detach --force-recreate

# Start streamlit services
docker compose -f ./setup/streamlit/docker-compose.yml down -v
docker compose -f setup/streamlit/docker-compose.yml up --build --detach --force-recreate
