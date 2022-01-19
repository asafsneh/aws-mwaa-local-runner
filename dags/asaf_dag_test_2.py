import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import base64
import logging

# read the file name
dag_name = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
    'owner': 'Asaf.Sneh',
    'email': 'asaf.sneh@is.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

direct_args = {
    'schedule_interval': '@daily',
    'default_args': default_args,
    'start_date': days_ago(1),
    'catchup': False,
    'description': 'test'
}

# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/



def get_secret():
    secret_name = "arn:aws:secretsmanager:us-east-1:032106861074:secret:mobile_data_eng/airflow/variables/asaf_dag_test_1-wdGcua"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])

    logging.info("*"*100)
    logging.info(secret)
    print(secret)
    return secret


with DAG(dag_name, **direct_args) as dag:
    start_task = DummyOperator(
        task_id='start_point'
    )

    end_task = DummyOperator(
        task_id='end_point'
    )

    test = PythonOperator(
        task_id='test',
        python_callable=get_secret,
        dag=dag,
    )

    start_task >> test >> end_task

dag.doc_md = """
# test
"""