FROM apache/airflow:3.1.0-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root
RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.12.0" "dbt-postgres~=1.11.0" \
    && chown -R airflow:0 /opt/dbt_venv
USER airflow

ENV PATH="${PATH}:/opt/dbt_venv/bin"
