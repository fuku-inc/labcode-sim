from typing import List, Dict
from prefect import task, flow
from fastapi import FastAPI, File, UploadFile, HTTPException
from timestamp import timestamp, timestamp_filename
# from time import sleep
from pathlib import Path
# from random import uniform
# from pathlib import Path
from log import OperationLog, TransportLog
from io import StringIO
from machines import HumanPlateServer, TecanFluent480, OpentronsOT2, TecanInfinite200Pro, HumanStoreLabware
# from lib_operator import Operator
# from .operator import Operator
import yaml
import requests
import random

LAB_SERVER_URL = 'http://log_server:8000'

app = FastAPI()

# class Operator:
#     id: str
#     type: str
#     task_input: List[str]
#     task_output: List[str]
#     storage_address: str

#     def __init__(self, id, type, manipulate_list):
#         self.id = id
#         self.type = type
#         # 該当するmanipulateが1つしかないことを想定している。
#         manipulate = [manipulate for manipulate in manipulate_list if manipulate['name'] == type][0]
#         self.task_input = [input['id'] for input in manipulate['input']]
#         self.task_output = [output['id'] for output in manipulate['output']]

#     def run(self):
#         metadata_path = Path(self.storage_address) / Path('metadata.json')
#         # ランダムな時間だけ待つ
#         running_time = uniform(1, 10)
#         sleep(running_time)
#         # save metadata
#         with open(metadata_path, 'w') as file:
#             file.write('{"metadata": "sample_metadata"}')
#         return "done"


class Conductor:
    def __init__(self, protocol, manipulates, machines):
        self.protocol = protocol
        self.manipulates = manipulates
        self.machines = machines
        self.connections = Conductor.connections_from_protocol(protocol)
        self.plan = Conductor.create_plan(self.connections)

    def connections_from_protocol(yaml_data) -> List[Dict[str, str]]:
        # with open(protocol_yaml_path, 'r') as file:
        #     yaml_data = yaml.safe_load(file)
        connections = yaml_data['connections']
        edge_list = [
            {
                'input_source': connection['input'][0],
                'input_content': connection['input'][1],
                'output_source': connection['output'][0],
                'output_content': connection['output'][1]
            } for connection in connections
        ]
        return edge_list

    def create_plan(connections: List[Dict[str, str]]) -> List[str]:
        """
        Create a plan from a protocol yaml file using a topological sort
        :param protocol_yaml_path: path to the protocol yaml file
        :return: a list of steps in the order they should
        """
        # make edge_list unique
        # edge_list = list(set(connections))
        edge_list = list(set([(connection['input_source'], connection['output_source']) for connection in connections]))
        node_list = list(set([edge[0] for edge in edge_list] + [edge[1] for edge in edge_list]))
        graph = {node: [] for node in node_list}
        for edge in edge_list:
            graph[edge[0]].append(edge[1])

        ret_list = []
        seen = {node: False for node in graph.keys()}

        def dfs(graph: Dict[str, str], node: str):
            seen[node] = True
            for child_node in graph[node]:
                if seen[child_node]:
                    continue
                dfs(graph, child_node)
            ret_list.append(node)

        [dfs(graph, node) for node in graph.keys() if not seen[node]]
        ret_list.reverse()
        return ret_list

    @flow
    def run(self):
        operators = self.protocol['operations']
        operator_type_dict = {operator['id']: operator['type'] for operator in operators}
        for operation_id in self.plan:
            if (operation_id != "input") & (operation_id != "output"):
                # print(f"run {operation_id}")
                suit_machine = random.choice([machine for machine in self.machines if machine.type == operator_type_dict[operation_id]])
                operation_log = OperationLog(
                    status="running",
                    start_time=timestamp(),
                    user_id="user_id",
                    lab_id="lab_id",
                    protocol_id="protocol_id",
                    task_id="task_id",
                    execution_id="execution_id",
                    storage_address="storage_address",  # conductorで指定する
                    operator_id=suit_machine.id,
                )
                # 開始時刻はここで指定する（ログサーバーでは指定しない）
                log_timestamp = requests.post(f'{LAB_SERVER_URL}/logs', json=operation_log.to_dict())
                status = suit_machine.run()
                log_timestamp = log_timestamp.json()['timestamp']
                print(log_timestamp)
                requests.patch(f'{LAB_SERVER_URL}/logs/{log_timestamp}', json={'status': status, 'end_time': timestamp()})
            # operator = operator_dict[operator_name]
            # status = operator.run()
            # requests.patch(f'{LAB_SERVER_URL}/logs/{log_timestamp}', data={'status': status})
            activated_connection = [connection for connection in self.connections if connection['input_source'] == operation_id]
            for connection in activated_connection:
                transport_log = TransportLog(
                    status="running",
                    start_time=timestamp(),
                    user_id="user_id",
                    lab_id="lab_id",
                    protocol_id="protocol_id",
                    source_task_id=connection['input_source'],
                    source_port_id=connection['input_content'],
                    destination_task_id=connection['output_source'],
                    destination_port_id=connection['output_content'],
                    operator_id=connection['input_source'],
                    execution_id="execution_id",
                    storage_address="storage_address",
                )
                transport_log_timestamp = requests.post(f'{LAB_SERVER_URL}/logs', json=transport_log.to_dict())
                transport_log_timestamp = transport_log_timestamp.json()['timestamp']
                print(transport_log_timestamp)
                # transport処理
                requests.patch(f'{LAB_SERVER_URL}/logs/{transport_log_timestamp}', json={'status': 'done', 'end_time': timestamp()})
                # self.update_log(timestamp, {'status': 'done'})
            # データ・ラボウェアのtransport


def read_uploaded_yaml(yaml_file: UploadFile = File(...)):
    if not yaml_file.filename.endswith(('.yaml', '.yml')):
        raise HTTPException(status_code=400, detail="Uploaded file must be a YAML file")
    try:
        # ファイルの内容を読み取る
        contents = yaml_file.file.read()
        contents_str = contents.decode('utf-8')
        # yamlファイルを読み取る
        yaml_data = yaml.safe_load(StringIO(contents_str))
        return yaml_data
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@app.post("/run_experiment")
def run_experiment(protocol_yaml: UploadFile = File(...), manipulate_yaml: UploadFile = File(...)):
    protocol = read_uploaded_yaml(protocol_yaml)
    manipulates = read_uploaded_yaml(manipulate_yaml)
    storage_address = Path("/app/storage") / Path(timestamp_filename())
    machines = [
        HumanPlateServer("human_plate_server", manipulates, storage_address),
        TecanFluent480("tecan_fluent_480", manipulates, storage_address),
        OpentronsOT2("opentrons_ot2", manipulates, storage_address),
        TecanInfinite200Pro("tecan_infinite_200_pro", manipulates, storage_address),
        HumanStoreLabware("human_store_labware", manipulates, storage_address),
    ]
    conductor = Conductor(protocol, manipulates, machines)
    conductor.run()
    # return machines[0]


if __name__ == '__main__':
    connections = Conductor.connections_from_protocol('protocol.yaml')
    # print(Conductor.create_plan(connections))
