# Use the official Airflow image as a template.
FROM apache/airflow:2.10.2

# Switch to root to install OS packages like git
USER root

# Install git and clean up afterwards
RUN apt-get update && apt-get install -y git && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy the requirements file into the container.
COPY requirements.txt .

# Install all dependencies from the requirements file.
RUN pip install -r requirements.txt

# Copy dbt directory into the container.
COPY dbt /opt/airflow/dbt
