from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from functions import db_utils

import os
from datetime import datetime


class MySqlToS3(BaseOperator):
    """
    The operator runs your query against MySQL,
    stores the file locally
    and then upload it to S3.

    :param mysql_conn_id - connection of the mySQL db
    :param aws_conn_id - connection to AWS account (default: aws_default)
    :param sql_query_path
    :param s3_path
    :param xcom_push (deafult True)
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#deb94b'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(
            self,
            mysql_conn_id,
            sql_query_path,
            s3_path,
            aws_conn_id='aws_default',
            is_xcom_push=True,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.s3_hook = S3Hook(aws_conn_id)
        self.sql_query_path = sql_query_path
        self.s3_path = s3_path
        self.is_xcom_push = is_xcom_push

    def execute(self, context):
        try:
            self.log.info("Dumping MySQL query results to local file")
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            s3_path = self.s3_path + ts + '/'

            with open(self.sql_query_path, 'r') as f:
                query = f.read()
            self.log.info(query)

            file_name = 'data_from_mysql_{ts}.csv.gz'.format(ts=ts)
            full_filename = "{path}/{f_name}".format(path=os.getcwd(), f_name=file_name)

            db_utils.execute_mysql_query(query, self.mysql_conn_id, full_filename)

            self.log.info(f"Started to upload file {full_filename} to S3: {s3_path}")
            self.s3_hook.load_file(
                filename=full_filename,
                key=s3_path + file_name
            )

            if self.is_xcom_push:
                self.xcom_push(context=context, key='s3_path', value=s3_path)
            self.log.info("Succeed to upload file to s3.")
            os.remove(full_filename)
        except Exception as e:
            print(str(e))
            raise
