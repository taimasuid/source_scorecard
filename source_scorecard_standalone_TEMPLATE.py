#confidential information has been removed from file
#must include proper information

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.executors.celery_executor import CeleryExecutor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

from jinja2 import Template
from jinja2 import contextfilter

DAG_NAME = '....-{{BRANCH}}'
OWNER = ''
OWNER_EMAIL = ''
BRANCH = '{{BRANCH}}'

@contextfilter
def recursive_render(context, value):
    rendered = Template(value).render(context)
    if '{{' in rendered:
        return recursive_render(context, rendered)
    else:
        return rendered

D = {
    'owner': OWNER,
    'depends_on_past': False,
    'email': [ OWNER_EMAIL ],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 1, 1),
    'task_concurrency': 8,
    'params': {

        'COUNTRY_CODE': 'us',
        'BRANCH': BRANCH,
        'PROJECT': '',
        'BUCKET': '',
        'REGION': '',
        'ZONE': '',
        'DATAPROC_IMAGE_PROJECT': '',
        'SOURCE_PREFIX': '',
        'PIPELINE_TYPE': '',
        'SERVICE_ACCOUNT': '',
        'SUBNET': '',

        'SOURCE_SCORECARD_COMMAND': 'python -tt run_source_scorecard.py {{ params.SOURCE_SCORECARD_SOURCE_NAMES }} {{ params.PROJECT }} {{ params.BUCKET }} {{ params.REGION }} {{ params.ZONE }} {{ params.SOURCE_PREFIX }} {{ params.BRANCH }}',
        'SOURCE_SCORECARD_DOCKER_IMAGE': 'gcr.io/[LOCATION]/[FILE NAME]-{{ params.BRANCH }}:latest',
        'SOURCE_SCORECARD_SOURCE_NAMES': 'sourceInput', 
        'SOURCE_SCORECARD_OUTPUT_NAME' : 'sourceScorecard',

        'LOAD_BQ_DOCKER_IMAGE' : '[LOAD TO BQ LOCATION]-{{ params.BRANCH }}:latest',
        'LOAD_BQ_COMMAND' : 'python -tt load_source_bq.py {{ params.SOURCE_SCORECARD_OUTPUT_NAME }} {{ params.PROJECT }} {{ params.BUCKET }} {{ params.REGION }} {{ params.ZONE }} {{ params.SOURCE_PREFIX }} {{ params.BRANCH }} --table_name=[TABLE LOCATION].[TABLE NAME]',

    }
}

def docker_run_operator(task_id, docker_image_param_name, docker_command_param_name, params={}, trigger_rule=None):
    params['cmd'] = 'gcloud auth configure-docker && \
        docker pull {{ params.%(docker_image)s }} && \
        docker run --rm -i \
            -e "PROJECT={{ params.PROJECT }}" \
            -e "SUBNET={{ params.SUBNET }}" \
            -e "BRANCH={{ params.BRANCH }}" \
            -e "REGION={{ params.REGION }}" \
            -e "ZONE={{ params.ZONE }}" \
            -e "BUCKET={{ params.BUCKET }}" \
            -e "DATA_PREFIX={{ params.SOURCE_PREFIX }}" \
            -e "SERVICE_ACCOUNT={{ params.SERVICE_ACCOUNT }}" \
            -e "RELEASE={{ params.RELEASE }}" \
            -e "COUNTRY_CODE={{ params.COUNTRY_CODE }}" \
            -e "DATAPROC_IMAGE_PROJECT={{ params.DATAPROC_IMAGE_PROJECT }}" \
            -e "DATAPROC_CLUSTER={{ params.DATAPROC_CLUSTER }}" \
            -e "LOCALIZED_CLUSTER_NAME={{ params.LOCALIZED_CLUSTER_NAME }}" \
            -e "PIPELINE_TYPE={{ params.PIPELINE_TYPE }}" \
            {{ params.%(docker_image)s }} {{ params.%(docker_command)s }}' % dict(
        docker_image=docker_image_param_name,
        docker_command=docker_command_param_name,
    )
    if trigger_rule == None:
        trigger_rule = TriggerRule.ALL_DONE if task_id.startswith('delete') and task_id.endswith('cluster') else TriggerRule.ALL_SUCCESS
    return BashOperator(task_id=task_id, bash_command='{{ params.cmd|recursive_render }}', params=params, trigger_rule=trigger_rule)


with DAG(DAG_NAME, default_args=dict(D), params={}, concurrency=16, catchup=False, schedule_interval=None, user_defined_filters=dict(recursive_render=recursive_render)) as dag:
    source_scorecard = docker_run_operator('source_scorecard', 'SOURCE_SCORECARD_DOCKER_IMAGE', 'SOURCE_SCORECARD_COMMAND')
    load_data_bq = docker_run_operator('load_data_bq', 'LOAD_BQ_DOCKER_IMAGE', 'LOAD_BQ_COMMAND')


    source_scorecard >> load_data_bq

