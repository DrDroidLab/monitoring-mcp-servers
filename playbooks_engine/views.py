from typing import Union

from django.http import HttpResponse
from google.protobuf.struct_pb2 import Struct
from google.protobuf.wrappers_pb2 import StringValue, BoolValue

from integrations.source_facade import source_facade
from protos.base_pb2 import Meta, TimeRange
from protos.playbooks.api_pb2 import RunPlaybookTaskRequest, RunPlaybookTaskResponse
from protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult
from protos.playbooks.playbook_pb2 import PlaybookTask, PlaybookTaskExecutionLog
from utils.decorators import account_post_api
from utils.proto_utils import get_meta
from utils.time_utils import current_epoch_timestamp


@account_post_api(RunPlaybookTaskRequest)
def task_run(request_message: RunPlaybookTaskRequest) -> Union[RunPlaybookTaskResponse, HttpResponse]:
    meta: Meta = request_message.meta
    time_range: TimeRange = meta.time_range
    if not time_range.time_lt or not time_range.time_geq:
        current_time = current_epoch_timestamp()
        time_range = TimeRange(time_geq=int(current_time - 14400), time_lt=int(current_time))

    task: PlaybookTask = request_message.playbook_task
    global_variable_set = Struct()
    if request_message.global_variable_set:
        global_variable_set = request_message.global_variable_set
    elif task.global_variable_set:
        global_variable_set = task.global_variable_set
    # interpreter_type: InterpreterType = task.interpreter_type if task.interpreter_type else InterpreterType.BASIC_I
    try:
        task_result = source_facade.execute_task(time_range, global_variable_set, task)
        # interpretation: InterpretationProto = task_result_interpret(interpreter_type, task, task_result)
        playbook_task_execution_log = PlaybookTaskExecutionLog(task=task, result=task_result,
                                                               execution_global_variable_set=global_variable_set)
    except Exception as e:
        playbook_task_execution_log = PlaybookTaskExecutionLog(task=task,
                                                               result=PlaybookTaskResult(
                                                                   error=StringValue(value=str(e))))
    return RunPlaybookTaskResponse(meta=get_meta(tr=time_range), success=BoolValue(value=True),
                                   playbook_task_execution_log=playbook_task_execution_log)
