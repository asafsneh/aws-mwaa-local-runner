import pandas
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jenkinsapi.jenkins import Jenkins, CrumbRequester
from functions import aws_utils
from airflow.models import Variable
from datetime import datetime
import requests
import json
import csv
import os


def execute_rs_query(query, conn_id, is_ddl=True):
    print("in execute rs query")

    try:
        redshift = PostgresHook(postgres_conn_id=conn_id)
        conn = redshift.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        print("after execute query")

        if not is_ddl:
            try:
                res = cur.fetchall()
                print(res)

                if len(res) == 0:
                    res = None
                return res
            except AttributeError as e:
                print("the query's result set does not match the expected"
                      " result set" + str(e))
                raise
        return None
    except Exception as e:
        print("exception" + str(e))
        raise


def execute_mysql_query(query, conn_id, filename=None):
    mysql = MySqlHook(mysql_conn_id=conn_id)

    conn = mysql.get_conn()
    try:
        if not filename:
            with conn.cursor() as cursor:
                cursor.execute(query)
                print("finished execution")
                results = cursor.fetchall()
                print("after results fetchall")

        else:
            print("before")
            results = pandas.read_sql_query(query, conn)
            print("before to csv")
            print(f"results size={len(results)}")
            results.to_csv(filename,
                           index=False,
                           header=False,
                           sep='^',
                           line_terminator='\n',
                           float_format='%d',
                           quoting=csv.QUOTE_NONNUMERIC,
                           quotechar='"',
                           doublequote='"',
                           escapechar="\\",
                           compression='gzip')
    except Exception as e:
        print("in exception" + str(e))
        raise
    return results


def unload_from_mysql(conn_id,
                      query_path,
                      s3_path,
                      filename='data.csv.gz',
                      table_name=None,
                      **context):
    try:
        print("Started csv download")
        MYSQL_PATH = os.getcwd()
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        s3_path += ts
        context['ti'].xcom_push(key='s3_path', value=s3_path)

        with open(query_path, 'r') as f:
            query = f.read()
        query = query.format(table_name=table_name) if table_name else query
        print(query)
        full_filename = "{path}/{name}".format(path=MYSQL_PATH, name=filename)
        print(full_filename)

        execute_mysql_query(query, conn_id, full_filename)

        print("Started csv upload")
        aws_utils.upload_file_to_s3(s3_path, MYSQL_PATH, filename)
    except Exception as e:
        print("exception" + str(e))
        raise


def copy_to_rs(conn_id,
               table_name,
               iam_role,
               copy_params,
               xcom_task_id=None,
               s3_path=None,
               query_path=None, **context):
    print("in copy")
    if not s3_path:
        s3_path = context['ti'].xcom_pull(key='s3_path',
                                          task_ids=xcom_task_id)

    if query_path:
        with open(query_path, 'r') as f:
            query = f.read()
    else:
        query = """COPY {table_name}
    from '{s3_path}/'
    iam_role '{iam_role}'
    {copy_parameters}
    ;
    commit;
    """

    query = query.format(table_name=table_name,
                         s3_path=s3_path,
                         iam_role=iam_role,
                         copy_parameters=copy_params
                         )
    print(query)
    execute_rs_query(query, conn_id)


def trigger_jenkins(job_name,
                    baseurl, username, password,
                    **job_kwargs):
    server = Jenkins(baseurl=baseurl, username=username, password=password,
                     requester=CrumbRequester(baseurl=baseurl, username=username, password=password))
    server.build_job(job_name, {**job_kwargs})


def send_victorops_alert(team,
                         process_name,
                         msg,
                         token):
    url = token + team
    values = {
        "message_type": "CRITICAL",
        "entity_id": process_name,
        "entity_display_name": process_name,
        "state_message": msg
    }
    try:
        request = requests.post(url, data=json.dumps(values))
        if request.status_code != 200:
            raise ValueError("error with sending VO alert: ", request.text)
    except Exception as e:
        print("VO Alert Unexpected Error: ", str(e))
        raise
    print(request.text)
