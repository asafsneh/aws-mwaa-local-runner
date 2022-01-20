import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from operators.mysql_to_s3_operator import MySqlToS3
from operators.s3_to_redshift_operator import S3toRedshift
from datetime import timedelta

# read the file name
dag_name = os.path.splitext(os.path.basename(__file__))[0]
dag_vars = Variable.get(dag_name, deserialize_json=True)

default_args = {
    'owner': 'Nastia.Vainberg',
    'email': dag_vars['failure_email'] if dag_vars.get('failure_email')
                                      else Variable.get('MOBILE_FAILURE_EMAIL'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
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

LOCAL_PATH = os.path.dirname(os.path.abspath(__file__))

unload_query_path = (LOCAL_PATH + '/') + \
                    (dag_vars['unload_query_path'] if 'unload_query_path'
                                            in dag_vars else dag_name+'.sql')

# take the default COPY_PARAMETERS if "copy_parameters": "default"
copy_params = Variable.get("COPY_PARAMETERS") \
    if dag_vars['copy_parameters'] == 'default'\
    else dag_vars['copy_parameters']

# take the default S3_PATH if there isnt s3_path in dag_vars
S3_PATH = (dag_vars['s3_path'] if dag_vars.get('s3_path')
           else Variable.get('S3_MAIN_PATH_MOBILE')) + dag_name + '/'


with DAG(dag_name, **direct_args) as dag:
    start_task = DummyOperator(
        task_id='start_point'
    )

    end_task = DummyOperator(
        task_id='end_point'
    )

    get_source_data = MySqlToS3(
        task_id='mysql_to_s3',
        mysql_conn_id=dag_vars['source_conn_id'],
        sql_query_path=unload_query_path,
        s3_path=S3_PATH
    )

    copy_to_dest = [S3toRedshift(
            task_id='copy_to_' + dest_redshift['dest_conn_id'],
            #provide_context=True,
            table_name=dest_redshift['dest_table'],
            redshift_conn_id=dest_redshift['dest_conn_id'],
            xcom_task_id='mysql_to_s3',
            xcom_key='s3_path',
            copy_params=copy_params
            )
        for dest_redshift in dag_vars['destination']]

    start_task >> get_source_data >> copy_to_dest  >> end_task

dag.doc_md = """
# ox_campaigns
Extracts data from Slave5a, table: openx.ox_campaigns
 and copies it to s3 and multiple Redshift clusters
"""