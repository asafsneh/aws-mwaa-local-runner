from airflow.plugins_manager import AirflowPlugin
from operators.mysql_to_s3_operator import MySqlToS3
from operators.s3_to_redshift_operator import S3toRedshift
from operators.jenkins_operator import JenkinsOperator
                    
class AirflowMobilePlugin(AirflowPlugin):
    name = 'airflow_mobile_plugin'
    operators = [MySqlToS3,S3toRedshift,JenkinsOperator]