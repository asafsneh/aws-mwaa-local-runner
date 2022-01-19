import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# read the file name
dag_name = os.path.splitext(os.path.basename(__file__))[0]
dag_vars = Variable.get(dag_name, deserialize_json=True)

default_args = {
    'owner': 'Asaf.Sneh',
    'email': dag_vars['failure_email'] if dag_vars.get('failure_email')
                                      else Variable.get('MOBILE_FAILURE_EMAIL'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

direct_args = {
    'schedule_interval': dag_vars['schedule_interval'],
    'default_args': default_args,
    'start_date': days_ago(1),
    'catchup': False,
    'description': '{is_critical}, max_delay={delay}, allowed_running_time={running_time}'.
        format(is_critical="critical" if dag_vars.get('victorops_team') else '',
               delay=dag_vars.get('max_delay'),
               running_time=dag_vars.get('allowed_running_time'))
}


def test():
    print(dag_vars)


with DAG(dag_name, **direct_args) as dag:
    start_task = DummyOperator(
        task_id='start_point'
    )

    end_task = DummyOperator(
        task_id='end_point'
    )

    test = PythonOperator(
        task_id='test',
        python_callable=test,
        dag=dag,
    )

    start_task >> test >> end_task

dag.doc_md = """
# ox_campaigns
Extracts data from Slave5a, table: openx.ox_campaigns
 and copies it to s3 and multiple Redshift clusters
"""