from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from jenkinsapi.jenkins import Jenkins, CrumbRequester
from airflow.hooks.base_hook import BaseHook


class JenkinsOperator(BaseOperator):
    """
    The operator triggers a jekins job.

    :param job_name - the name in jenkins
    :param baseurl - jenkins host
    :param username
    :param password
    :param  **job_kwargs - dictionary of the job
                           parameter names and their values
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#f9ec86'
    ui_fgcolor = '#000000'

    @apply_defaults
    def __init__(self,
                 job_name,
                 job_kwargs,
                 baseurl=None,
                 username=None,
                 password=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        jenkins_server = BaseHook.get_connection(conn_id='jenkins_mobile')
        self.job_name=job_name
        self.baseurl=baseurl if baseurl else jenkins_server.host
        self.username=username if username else jenkins_server.login
        self.password=password if password else jenkins_server.password
        self.job_kwargs=job_kwargs

    def execute(self, context):
        server = Jenkins(baseurl=self.baseurl,
                         username=self.username,
                         password=self.password,
                         requester=CrumbRequester(baseurl=self.baseurl,
                                                  username=self.username,
                                                  password=self.password,))
        server.build_job(self.job_name,self.job_kwargs)
