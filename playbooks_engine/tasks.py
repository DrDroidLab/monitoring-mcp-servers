import logging

import requests
from celery import shared_task
from django.conf import settings
from google.protobuf.struct_pb2 import Struct

from integrations.source_facade import source_facade
from protos.base_pb2 import TimeRange
from protos.playbooks.playbook_pb2 import PlaybookTask
from utils.proto_utils import dict_to_proto, proto_to_dict

logger = logging.getLogger(__name__)


@shared_task(max_retries=3, default_retry_delay=10)
def fetch_playbook_execution_tasks():
    drd_cloud_host = settings.DRD_CLOUD_API_HOST
    drd_cloud_api_token = settings.DRD_CLOUD_API_TOKEN

    tasks = []
    response = requests.post(f'{drd_cloud_host}/playbooks-engine/proxy/execution/tasks',
                             headers={'Authorization': f'Bearer {drd_cloud_api_token}'}, json={})
    if response.status_code != 200:
        logger.error(f'fetch_playbook_execution_tasks:: Failed to get scheduled tasks with DRD '
                     f'Cloud: {response.json()}')
        return False
    playbook_task_executions = response.json().get('playbook_task_executions', [])
    for pet in playbook_task_executions:
        task = pet.get('task', {})


@shared_task(max_retries=3, default_retry_delay=10)
def execute_task_and_send_result(playbook_task_execution_log):
    try:
        drd_cloud_host = settings.DRD_CLOUD_API_HOST
        drd_cloud_api_token = settings.DRD_CLOUD_API_TOKEN

        task = playbook_task_execution_log.get('task', {})
        task_proto = dict_to_proto(task, PlaybookTask)

        time_range_dict = playbook_task_execution_log.get('time_range', {})
        time_rance = dict_to_proto(time_range_dict, TimeRange)

        global_variable_set_dict = playbook_task_execution_log.get('execution_global_variable_set', {})
        global_variable_set = dict_to_proto(global_variable_set_dict, Struct) if global_variable_set_dict else Struct()
        result = source_facade.execute_task(time_rance, task_proto, global_variable_set)
        result_dict = proto_to_dict(result)
        playbook_task_execution_log['result'] = result_dict

        response = requests.post(f'{drd_cloud_host}/playbooks-engine/proxy/execution/result',
                                 headers={'Authorization': f'Bearer {drd_cloud_api_token}'},
                                 json={'playbook_task_execution_logs': [playbook_task_execution_log]})
    except Exception as e:
        logger.error(f'Error while executing task: {str(e)}')
