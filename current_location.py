from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_current_directory():
    current_directory = os.getcwd()
    print(f"Current working directory: {current_directory}")

    # Specify the directory you want to list
    directory = current_directory

    # List all files in the specified directory
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    print(files)

    # List all directories in the specified directory
    folders = [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]

    print(folders)

    directory = current_directory + '/dags/repo'

    # List all files in the specified directory
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    print(files)

    # List all directories in the specified directory
    folders = [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]

    print(folders)

    directory = current_directory + '/dags/repo/dags'

    # List all files in the specified directory
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]

    print(files)

    # List all directories in the specified directory
    folders = [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]

    print(folders)

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 14),
    'retries': 1,
}

# Create the DAG
with DAG(
    dag_id='print_current_directory_dag',
    default_args=default_args,
    schedule_interval='@once',  # Runs only once
    catchup=False,
) as dag:
    
    # Define the task
    print_dir_task = PythonOperator(
        task_id='print_current_directory',
        python_callable=print_current_directory,
    )

# Set task dependencies (if needed)
print_dir_task
