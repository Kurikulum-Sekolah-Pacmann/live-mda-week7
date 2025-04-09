#!/bin/bash

# Load environment variables from .env file
set -a
source ./setup/sources/.env
set +a

echo "🔌 Registering Debezium connector for database: $SRC_POSTGRES_DB"

curl -X POST http://localhost:8084/connectors \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "name": "postgres-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "sources",
    "database.port": "5432",
    "database.user": "$SRC_POSTGRES_USER",
    "database.password": "$SRC_POSTGRES_PASSWORD",
    "database.dbname": "$SRC_POSTGRES_DB",
    "database.server.name": "pgserver",
    "slot.name": "debezium",
    "publication.name": "dbz_publication",
    "table.include.list": "production.*,sales.*",
    "topic.prefix": "source",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "decimal.handling.mode": "string"
  }
}
EOF

echo "✅ Debezium connector registered successfully."
echo "⏳ Waiting for Debezium connector to start..."
