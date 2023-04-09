FROM  prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt create_bq_tables.py ./
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org
