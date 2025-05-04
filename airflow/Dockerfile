# Base Airflow image
FROM apache/airflow:2.9.1

# Environment configuration
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080

RUN mkdir -p /opt/airflow/credentials

# Copy the service account key file into the container
COPY credentials/service-account.json /opt/airflow/credentials/service-account.json

# Switch to the airflow user
USER airflow

# Copy and install requirements
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt

# Copy DAGs
COPY ./dags ${AIRFLOW_HOME}/dags

# Ensure airflow home exists
RUN mkdir -p ${AIRFLOW_HOME}

# Initialize the database
RUN airflow db init

# Create default user (optional)
RUN airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Run both scheduler and webserver in the same container
CMD ["bash", "-c", "airflow scheduler & airflow webserver"]