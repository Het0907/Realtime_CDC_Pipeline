FROM apache/airflow:2.10.5-python3.11

USER root
COPY --chown=airflow:root airflow/requirements.txt /tmp/project-requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /tmp/project-requirements.txt
