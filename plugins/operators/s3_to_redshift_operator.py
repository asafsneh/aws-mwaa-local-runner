from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from functions import db_utils
from airflow.models import Variable


class S3toRedshift(BaseOperator):
    """
    Copies data from S3 to Redshift. The operator runs your query against
    Redshift, and copy the data from S3 path.

    :param table_name - table in RS to load data into
    :param redshift_conn_id - connection id representing the relevant RS
    :param s3_path
    :param xcom_task_id
    :param xcom_key - name of param to pull from xcom
    :param copy_params
    :param query_path - path of query query
    """
    ui_color = '#de4b81'
    ui_fgcolor = '#fff'
    template_fields = ()
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            table_name,
            redshift_conn_id,
            s3_path=None,
            xcom_task_id=None,
            xcom_key=None,
            copy_params=None,
            query_path=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key
        self.copy_params = copy_params
        self.query_path = query_path

    def execute(self, context):
        if (not self.s3_path) and self.xcom_task_id:
            self.log.info(f"Pull s3 path from xcom. task: {self.xcom_key} key: {self.xcom_task_id}")
            self.s3_path = self.xcom_pull(context=context,
                                          key=self.xcom_key,
                                          task_ids=self.xcom_task_id)

        self.log.info(f"Start to copy data from s3 {self.s3_path} to Redshift")

        if self.query_path:
            with open(self.query_path, 'r') as f:
                query = f.read()
        else:
            query = """DELETE FROM {table_name};
        COPY {table_name}
        from '{s3_path}'
        iam_role '{iam_role}'
        {copy_parameters}
        ;
        commit;
        """
        query = query.format(table_name=self.table_name,
                             s3_path=self.s3_path,
                             iam_role=self.get_iam_role(self.redshift_conn_id),
                             copy_parameters=self.copy_params
                             )
        self.log.info(query)
        db_utils.execute_rs_query(query, self.redshift_conn_id)
        self.log.info("Copy succeed.")

    def get_iam_role(self, redshift_conn_id):
        if redshift_conn_id == 'bi-dwh':
            return Variable.get("AWS_MOBILE_ROLE_FOR_DWH") + "," + Variable.get("AWS_MOBILE_ROLE")
        else:
            return Variable.get("AWS_MOBILE_ROLE")
