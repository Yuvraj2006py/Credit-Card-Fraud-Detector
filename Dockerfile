FROM apache/airflow:2.7.3

# Install extra Python dependencies
COPY requirements-airflow.txt .
RUN pip install --no-cache-dir -r requirements-airflow.txt
