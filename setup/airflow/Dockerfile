FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt

COPY ./variables_and_connections /init/variables_and_connections
RUN pip install --no-cache-dir -r /requirements.txt

RUN python -m virtualenv venv && source venv/bin/activate 