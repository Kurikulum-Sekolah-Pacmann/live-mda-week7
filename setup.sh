#!/bin/bash

# Start all airflow services
docker compose -f ./setup/airflow/docker-compose.yml down -v
docker compose -f ./setup/airflow/docker-compose.yml up --build --detach --force-recreate

# Import airflow variables and connections
docker exec -it airflow-webserver airflow connections import /init/variables_and_connections/airflow_connections_init.yaml
docker exec -it airflow-webserver airflow variables import -a overwrite /init/variables_and_connections/airflow_variables_init.json

# Start monitoring services
docker compose -f ./setup/airflow-monitoring/docker-compose.yml down -v
docker compose -f ./setup/airflow-monitoring/docker-compose.yml up --build --detach --force-recreate

# Start data lake services
docker compose -f ./setup/data-lake/docker-compose.yml down -v
docker compose -f ./setup/data-lake/docker-compose.yml up --build --detach --force-recreate


# Start data sources services
docker compose -f ./setup/sources/docker-compose.yml down -v
docker compose -f ./setup/sources/docker-compose.yml up --build --detach --force-recreate

# Start data warehouse services
docker compose -f ./setup/warehouse/docker-compose.yml down -v
docker compose -f setup/warehouse/docker-compose.yml up --build --detach --force-recreate

# Start metabase service
docker compose -f ./setup/metabase/docker-compose.yml down -v
docker compose -f setup/metabase/docker-compose.yml up --build --detach --force-recreate

# Start kafka services
docker compose -f ./setup/kafka/docker-compose.yml down -v
docker compose -f ./setup/kafka/docker-compose.yml up --build --detach --force-recreate

# Create Connector Debezium
./setup/sources/connector.sh