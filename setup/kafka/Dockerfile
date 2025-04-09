FROM debezium/connect:2.4

# Add the PostgreSQL connector plugin
RUN mkdir -p /kafka/connect/debezium-plugins \
    && cd /kafka/connect/debezium-plugins \
    && curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.4.1.Final/debezium-connector-postgres-2.4.1.Final-plugin.tar.gz \
    | tar xz

ENV CONNECT_PLUGIN_PATH="/kafka/connect/debezium-plugins"
