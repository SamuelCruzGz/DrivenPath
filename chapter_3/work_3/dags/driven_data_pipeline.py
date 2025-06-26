import csv
import random
import csv
import logging
import uuid
import polars as pl

from faker import Faker
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Configure logging.
logging.basicConfig(
    level=logging.INFO,                    
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


def _create_data(locale: str) -> Faker:
    """
    Creates a Faker instance for generating localized fake data.
    Args:
        locale (str): The locale code for the desired fake data language/region.
    Returns:
        Faker: An instance of the Faker class configured with the specified locale.
    """
    # Log the action.
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)


def _generate_record(fake: Faker) -> list:
    """
    Generates a single fake user record.
    Args:
        fake (Faker): A Faker instance for generating random data.
    Returns:
        list: A list containing various fake user details such as name, username, email, etc.
    """
    # Generate random personal data.
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()  # Create a lowercase username without spaces.
    email = f"{user_name}@{fake.free_email_domain()}"  # Combine the username with a random email domain.
    personal_number = fake.ssn()  # Generate a random social security number.
    birth_date = fake.date_of_birth()  # Generate a random birth date.
    address = fake.address().replace("\n", ", ")  # Replace newlines in the address with commas.
    phone_number = fake.phone_number()  # Generate a random phone number.
    mac_address = fake.mac_address()  # Generate a random MAC address.
    ip_address = fake.ipv4()  # Generate a random IPv4 address.
    iban = fake.iban()  # Generate a random IBAN.
    accessed_at = fake.date_time_between("-1y")  # Generate a random date within the last year.
    session_duration = random.randint(0, 36_000)  # Random session duration in seconds (up to 10 hours).
    download_speed = random.randint(0, 1_000)  # Random download speed in Mbps.
    upload_speed = random.randint(0, 800)  # Random upload speed in Mbps.
    consumed_traffic = random.randint(0, 2_000_000)  # Random consumed traffic in kB.

    # Return all the generated data as a list.
    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]


def _write_to_csv() -> None:
    """
    Genera registros falsos y los escribe en CSV, sobrescribiendo el archivo cada vez.
    """
    fake = _create_data("es_ES")
    
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    if str(date.today()) == "2024-09-23":
        rows = 100_372
    else:
        rows = random.randint(0, 1_101)
    
    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        for _ in range(rows):
            record = _generate_record(fake)
            # Convertir fechas a string para que sean consistentes
            record[4] = record[4].strftime("%Y-%m-%d")  # birth_date
            record[10] = record[10].strftime("%Y-%m-%d %H:%M:%S")  # accessed_at
            writer.writerow(record)
    
    logging.info(f"Written {rows} records to the CSV file.")



def _add_id() -> None:
    """
    Agrega una columna UUID única a cada fila del CSV.
    """
    df = pl.read_csv(
        "/opt/airflow/data/raw_data.csv",
        dtypes={
            "person_name": pl.Utf8,
            "user_name": pl.Utf8,
            "email": pl.Utf8,
            "personal_number": pl.Utf8,
            "birth_date": pl.Utf8,
            "address": pl.Utf8,
            "phone": pl.Utf8,
            "mac_address": pl.Utf8,
            "ip_address": pl.Utf8,
            "iban": pl.Utf8,
            "accessed_at": pl.Utf8,
            "session_duration": pl.Int64,
            "download_speed": pl.Int64,
            "upload_speed": pl.Int64,
            "consumed_traffic": pl.Int64
        },
        ignore_errors=True
    )
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    df.write_csv("/opt/airflow/data/raw_data.csv")
    logging.info("Added UUID to the dataset.")



def _update_datetime() -> None:
    """
    Actualiza la columna 'accessed_at' en el CSV con el timestamp adecuado.
    """
    if str(date.today()) != "2024-09-23":
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = (current_time - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        df = pl.read_csv(
            "/opt/airflow/data/raw_data.csv",
            dtypes={
                "person_name": pl.Utf8,
                "user_name": pl.Utf8,
                "email": pl.Utf8,
                "personal_number": pl.Utf8,
                "birth_date": pl.Utf8,
                "address": pl.Utf8,
                "phone": pl.Utf8,
                "mac_address": pl.Utf8,
                "ip_address": pl.Utf8,
                "iban": pl.Utf8,
                "accessed_at": pl.Utf8,
                "session_duration": pl.Int64,
                "download_speed": pl.Int64,
                "upload_speed": pl.Int64,
                "consumed_traffic": pl.Int64,
                "unique_id": pl.Utf8
            },
            ignore_errors=True
        )
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        df.write_csv("/opt/airflow/data/raw_data.csv")
        logging.info("Updated accessed timestamp.")



def save_raw_data():
    '''
    Execute all steps for data generation.
    '''
    # Logging starting of the process.
    logging.info(f"Started batch processing for {date.today()}.")
    # Generate and write records to the CSV.
    _write_to_csv()
    # Add UUID to dataset.
    _add_id()
    # Update the timestamp.
    _update_datetime()
    # Logging ending of the process.
    logging.info(f"Finished batch processing {date.today()}.")


# Define the default arguments for DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Define the DAG.
dag = DAG(
    'extract_raw_data_pipeline',
    default_args=default_args,
    description='DataDriven Main Pipeline.',
    schedule_interval="* 7 * * *",
    start_date=datetime(2024, 9, 22),
    catchup=False,
)

# Define extract raw data task.
extract_raw_data_task = PythonOperator(
    task_id='extract_raw_data',
    python_callable=save_raw_data,
    dag=dag,
)

# Define create raw schema task.
create_raw_schema_task = SQLExecuteQueryOperator(
    task_id='create_raw_schema',
    conn_id='postgres_conn',
    sql='CREATE SCHEMA IF NOT EXISTS driven_raw;',
    dag=dag,
)

# Define create raw table task.
create_raw_table_task = SQLExecuteQueryOperator(
    task_id='create_raw_table',
    conn_id='postgres_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
            person_name VARCHAR(100),
            user_name VARCHAR(100),
            email VARCHAR(100),
            personal_number VARCHAR(100), 
            birth_date VARCHAR(100), 
            address VARCHAR(100),
            phone VARCHAR(100), 
            mac_address VARCHAR(100),
            ip_address VARCHAR(100),
            iban VARCHAR(100),
            accessed_at TIMESTAMP,
            session_duration VARCHAR(100),
            download_speed VARCHAR(100),
            upload_speed VARCHAR(100),
            consumed_traffic VARCHAR(100),
            unique_id VARCHAR(100)
        );
    """,
    dag=dag
)

# Define load CSV data into the table task.
load_raw_data_task = SQLExecuteQueryOperator(
    task_id='load_raw_data',
    conn_id='postgres_conn',
    sql="""
    COPY driven_raw.raw_batch_data(
    person_name, user_name, email, personal_number, birth_date,
    address, phone, mac_address, ip_address, iban, accessed_at,
    session_duration, download_speed, upload_speed, consumed_traffic, unique_id
    ) 
    FROM '/opt/airflow/data/raw_data.csv' 
    DELIMITER ',' 
    CSV HEADER;
    """
)

# Define staging dbt models run task.
run_dbt_staging_task = BashOperator(
    task_id='run_dbt_staging',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:staging',
)

# Define trusted dbt models run task.
run_dbt_trusted_task = BashOperator(
    task_id='run_dbt_trusted',
    bash_command='set -x; cd /opt/airflow/dbt && dbt run --select tag:trusted',
)

# Set the task in the DAG
[extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
create_raw_table_task >> load_raw_data_task >> run_dbt_staging_task
run_dbt_staging_task >> run_dbt_trusted_task
