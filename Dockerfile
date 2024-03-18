FROM apache/airflow:2.7.3

COPY requirements.txt /opt/requirements.txt

RUN pip install -r /opt/requirements.txt \
    && pip install --upgrade oauth2client
