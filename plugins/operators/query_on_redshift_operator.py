from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from functions import db_utils
from airflow.models import Variable


class QueryOnRedshift(BaseOperator):
    """
    The operator runs your query against Redshift
    and copy the data from S3 path.

    :param redshift_conn_id - connection ID to RS,
    :param query_path=None
    :param query_string=None

    """
    ui_color = '#de4b81'
    ui_fgcolor = '#fff'
    template_fields = ()
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            redshift_conn_id,
            query_path=None,
            query_string=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_path = query_path
        self.query_string = query_string

    def execute(self, context):
        self.log.info(f"Execute query on Redshift")

        if self.query_path and (not self.query_string):
            with open(self.query_path, 'r') as f:
                query = f.read()

        elif (not self.query_path) and self.query_string:
            query = self.query_string

        else:
            error_msg = "Both query path and query string are empty, there is no query to run."
            self.log.error(error_msg)
            raise ValueError(error_msg)

        self.log.info(query)
        db_utils.execute_rs_query(query, self.redshift_conn_id)
        self.log.info("Query succeed.")
