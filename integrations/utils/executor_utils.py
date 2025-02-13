from typing import Dict

from google.protobuf.struct_pb2 import Struct

from integrations.source_api_processors.lambda_function_processor import LambdaFunctionProcessor
from protos.literal_pb2 import LiteralType
from protos.playbooks.source_task_definitions.lambda_function_task_pb2 import Lambda
from protos.ui_definition_pb2 import FormField


def apply_result_transformer(result_dict, lambda_function: Lambda.Function) -> Dict:
    lambda_function_processor = LambdaFunctionProcessor(lambda_function.definition.value,
                                                        lambda_function.requirements)
    transformer_result = lambda_function_processor.execute(result_dict)
    if not isinstance(transformer_result, Dict):
        raise ValueError("Result transformer should return a dictionary")
    transformer_result = {f"${k}" if not k.startswith("$") else k: v for k, v in transformer_result.items()}
    return transformer_result


def resolve_global_variables(form_fields: [FormField], global_variable_set: Struct,
                             source_type_task_def: Dict) -> (Dict, Dict):
    all_string_fields = [ff.key_name.value for ff in form_fields if ff.data_type == LiteralType.STRING]
    all_string_array_fields = [ff.key_name.value for ff in form_fields if ff.data_type == LiteralType.STRING_ARRAY]
    all_composite_fields = {}
    for ff in form_fields:
        if ff.is_composite:
            all_composite_fields[ff.key_name.value] = ff.composite_fields

    task_local_variable_map = {}
    for gk, gv in global_variable_set.items():
        for tk, tv in source_type_task_def.items():
            if tk in all_string_fields:
                if gv is None:
                    raise Exception(f"Global variable {gk} is None")
                source_type_task_def[tk] = tv.replace(gk, gv)
                if gk in tv:
                    task_local_variable_map[gk] = gv
            elif tk in all_string_array_fields:
                for item in source_type_task_def[tk]:
                    if gv is None:
                        raise Exception(f"Global variable {gk} is None")
                    source_type_task_def[tk] = item.replace(gk, gv) if isinstance(tv, str) else item
                if gk in tv:
                    task_local_variable_map[gk] = gv
            elif tk in all_composite_fields:
                composite_fields = all_composite_fields[tk]
                for item in source_type_task_def[tk]:
                    for cf in composite_fields:
                        if cf.data_type == LiteralType.STRING:
                            if gv is None:
                                raise Exception(f"Global variable {gk} is None")
                            item[cf.key_name.value] = item[cf.key_name.value].replace(gk, gv) if isinstance(tv, str) else item
                if gk in tv:
                    task_local_variable_map[gk] = gv
    return source_type_task_def, task_local_variable_map
