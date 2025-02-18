from typing import List, Dict, TypedDict
from fastapi import FastAPI, File, UploadFile, HTTPException
from timestamp import timestamp, timestamp_filename
# from time import sleep
from pathlib import Path
# from random import uniform
# from pathlib import Path
from log import OperationLog, TransportLog
from io import StringIO
from machines import HumanPlateServer, TecanFluent480, OpentronsOT2, TecanInfinite200Pro, HumanStoreLabware
from util import calculate_md5
from lib_operator import Operator
# from lib_operator import Operator
# from .operator import Operator
import yaml
import requests
import random

LOG_SERVER_URL = 'http://log_server:8000'

app = FastAPI()


class Connection(TypedDict):
    input_source: str
    input_content: str
    output_source: str
    output_content: str
    is_data: bool


class OperationsInProtocol(TypedDict):
    id: str
    type: str


class Operation:
    db_id: int
    process_db_id: int
    process_name: str
    name: str
    started_at: str | None
    finished_at: str | None
    status: str
    storage_address: str
    is_transport: bool
    is_data: bool

    def __init__(
            self,
            process_db_id,
            process_name,
            name,
            storage_address,
            is_transport,
            is_data
    ):
        self.process_db_id = process_db_id
        self.process_name = process_name
        self.name = name
        self.started_at = None
        self.finished_at = None
        self.status = "not started"
        self.storage_address = storage_address
        self.is_transport = is_transport
        self.is_data = is_data

    def post(self):
        response = requests.post(
            url=f'{LOG_SERVER_URL}/operations/',
            data={
                "process_id": self.process_db_id,
                "name": self.name,
                "status": self.status,
                "storage_address": self.storage_address,
                "is_transport": self.is_transport,
                "is_data": self.is_data
            }
        )
        self.db_id = response.json()["id"]

    def set_process_db_id(self, process_db_id):
        self.process_db_id = process_db_id


class Process:
    db_id: int
    run_id: int
    type: str
    id_in_protocol: str
    storage_address: str

    def __init__(self, run_id, type, id_in_protocol, storage_address):
        self.run_id = run_id
        self.type = type
        self.id_in_protocol = id_in_protocol
        self.storage_address = storage_address

    def post(self):
        response = requests.post(
            url=f'{LOG_SERVER_URL}/processes/',
            data={
                "run_id": self.run_id,
                "storage_address": self.storage_address
            }
        )
        self.db_id = response.json()["id"]

    def operation_mapping(self, machines: List[Operator]) -> Operation:
        if self.id_in_protocol in ["input", "output"]:
            operation = Operation(
                process_db_id=self.db_id,
                process_name=self.id_in_protocol,
                name=self.id_in_protocol,
                storage_address='storage/operation',
                is_transport=False,
                is_data=False
            )
            return operation
        suit_machine = random.choice([machine for machine in machines if machine.type == self.type])
        operation = Operation(
            process_db_id=self.db_id,
            process_name=self.id_in_protocol,
            name=suit_machine.id,
            storage_address='storage/operation',
            is_transport=False,
            is_data=False
        )
        return operation


def connection_to_operation(connection_list: List[Connection], process_list: List[Process], operation_list: List[Operation]):
    connections = [{
        "input_source": connection['input'][0],
        "input_content": connection['input'][1],
        "output_source": connection['output'][0],
        "output_content": connection['output'][1],
        "is_data": connection['is_data']
    } for connection in connection_list]
    operation_list_from_connection = []
    edge_list = []
    for connection in connections:
        source_process = [process for process in process_list if process.id_in_protocol == connection['input_source']][0]
        operation = Operation(
            process_db_id=source_process.db_id,
            process_name=source_process.id_in_protocol,
            name=f"{connection['input_source']}_{connection['input_content']}_{connection['output_source']}_{connection['output_content']}",
            storage_address='storage/operation',
            is_transport=True,
            is_data=connection["is_data"]
        )
        operation_name_from = [operation.name for operation in operation_list if operation.process_name == connection['input_source']][0]
        operation_name_to = [operation.name for operation in operation_list if operation.process_name == connection['output_source']][0]
        operation_list_from_connection.append(operation)
        if connection["is_data"]:
            edge_list.append({"from": operation_name_from, "to": operation_name_to})
        else:
            edge_list.append({"from": operation_name_from, "to": operation.name})
            edge_list.append({"from": operation.name, "to": operation_name_to})

    return operation_list_from_connection, edge_list


def create_process_and_operation_and_edge(run_id, protocol_dict, machines):
    processes = protocol_dict["operations"]
    connections = protocol_dict["connections"]

    process_list = [
        Process(
            run_id=run_id,
            type=process["type"],
            id_in_protocol=process["id"],
            storage_address=f'process/{process["id"]}'
        ) for process in processes
    ]

    input_process = Process(
        run_id=run_id,
        type="input",
        id_in_protocol="input",
        storage_address=""
    )
    output_process = Process(
        run_id=run_id,
        type="output",
        id_in_protocol="output",
        storage_address=""
    )

    process_list += [input_process, output_process]
    [process.post() for process in process_list]

    operation_list = [process.operation_mapping(machines=machines) for process in process_list]
    operation_list_from_connection, edge_list = connection_to_operation(connections, process_list, operation_list)
    operation_list += operation_list_from_connection
    [operation.post() for operation in operation_list]

    edge_db_id_list = []
    print(edge_list)
    for edge in edge_list:
        operation_db_id_from = [operation.db_id for operation in operation_list if operation.name == edge["from"]][0]
        operation_db_id_to = [operation.db_id for operation in operation_list if operation.name == edge["to"]][0]
        edge_db_id_list.append({
            "from": operation_db_id_from,
            "to": operation_db_id_to
        })

    for edge in edge_db_id_list:
        response = requests.post(
            url=f'{LOG_SERVER_URL}/edges/',
            data={
                "run_id": run_id,
                "from_id": edge["from"],
                "to_id": edge["to"]
            }
        )
        print(response.json())
    # print(edge_db_id_list)


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


def calc_parent_id(connections: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """
    DAGのコネクションを統一して辞書を作成し、親子関係を表すparent_idを設定する。
    
    :param connections: コネクションのリスト ({'id': connection_id, 'input': id, 'output': id}の辞書)
    :return: 統一されたノード辞書
    """
    processed_nodes: Dict[str, Dict[str, str]] = {}
    
    all_nodes = set()
    for conn in connections:
        all_nodes.add(conn['input_source'])
        all_nodes.add(conn['output_source'])
    
    for node in all_nodes:
        processed_nodes[node] = {'id': node, 'parent_id': None}
    
    for conn in connections:
        conn_id = conn['id']  # 既存のIDを使用
        processed_nodes[conn_id] = {'id': conn_id, 'parent_id': conn['input_source']}
        
        # 出力ノードの親IDをコネクションIDに設定
        processed_nodes[conn['output_source']]['parent_id'] = conn_id
    
    return processed_nodes


class Conductor:
    def __init__(self, project_id, protocol_name, protocol, checksum, user_id, manipulates, machines, storage_address):
        self.project_id = project_id
        self.protocol_name = protocol_name
        self.protocol = protocol
        self.checksum = checksum
        self.user_id = user_id
        self.manipulates = manipulates
        self.machines = machines
        self.connections = Conductor.connections_from_protocol(protocol)
        self.plan = Conductor.create_plan(self.connections)
        self.storage_address = storage_address
        response = requests.post(
            url=f'{LOG_SERVER_URL}/runs/',
            data={
                "project_id": self.project_id,
                "file_name": self.protocol_name,
                "checksum": self.checksum,
                "user_id": self.user_id,
                "storage_address": self.storage_address
            }
        )
        self.id = response.json()["id"]

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

    async def register_operations(self):
        operators = self.protocol['operations']
        process_list = [
            {
                "run_id": self.id,
                "name": operator['id'],
                "storage_address": f'storage/processes/{operator["id"]}'
            } for operator in operators]
        for process in process_list:
            # プロセスの登録
            response = requests.post(f'{LOG_SERVER_URL}/processes/', data=process)
            process['id'] = response.json()['id']
        connection_with_id = [{
            "id": i,
            "input_source": connection['input_source'],
            "input_content": connection['input_content'],
            "output_source": connection['output_source'],
            "output_content": connection['output_content']
        } for i, connection in enumerate(self.connections)]
        parent_id_list = calc_parent_id(connection_with_id)
        operation_list = [{
            "process_id": process['id'],
            "name": process['name'],
            "parent_id": parent_id_list[process['name']]['parent_id'],
            "status": "not started",
            "storage_address": f'storage_address/operations/{process["name"]}',
        } for process in process_list]
        connection_list = [{
            "process_id": connection['input_source'],
            "name": f"transport {connection['input_content']} to {connection['output_content']}",
            "parent_id": parent_id_list[connection['id']]['parent_id'],
            "status": "not started",
            "storage_address": f'storage_address/operations/conn_{connection["id"]}',
        } for connection in connection_with_id]
        print(operation_list[3])
        print(connection_list[0])
        operation_list = operation_list + connection_list
        for operation in operation_list:
            response = requests.post(f'{LOG_SERVER_URL}/operations/', data=operation)
            # operation['id'] = response.json()['id']
        # operator_type_dict = {operator['id']: operator['type'] for operator in operators}
        # for operation_id in self.plan:
        #     if (operation_id != "input") & (operation_id != "output"):
        #         # print(f"run {operation_id}")
        #         suit_machine = random.choice([machine for machine in self.machines if machine.type == operator_type_dict[operation_id]])
        #         data = {
        #             "process_id": 1,
        #             "name": suit_machine.id,
        #             "status": "not started",
        #             "storage_address": "storage_address",  # conductorで指定する
        #         }
        #         # 開始時刻はここで指定する（ログサーバーでは指定しない）
        #         log_timestamp = requests.post(f'{LOG_SERVER_URL}/logs', json=operation_log.to_dict())
        #         status = suit_machine.run()
        #         log_timestamp = log_timestamp.json()['timestamp']
        #         print(log_timestamp)
        #         requests.patch(f'{LOG_SERVER_URL}/logs/{log_timestamp}', json={'status': status, 'end_time': timestamp()})
        #     # operator = operator_dict[operator_name]
        #     # status = operator.run()
        #     # requests.patch(f'{LAB_SERVER_URL}/logs/{log_timestamp}', data={'status': status})
        #     activated_connection = [connection for connection in self.connections if connection['input_source'] == operation_id]
        #     for connection in activated_connection:
        #         transport_log = TransportLog(
        #             status="running",
        #             start_time=timestamp(),
        #             user_id="user_id",
        #             lab_id="lab_id",
        #             protocol_id="protocol_id",
        #             source_task_id=connection['input_source'],
        #             source_port_id=connection['input_content'],
        #             destination_task_id=connection['output_source'],
        #             destination_port_id=connection['output_content'],
        #             operator_id=connection['input_source'],
        #             execution_id="execution_id",
        #             storage_address="storage_address",
        #         )
        #         transport_log_timestamp = requests.post(f'{LOG_SERVER_URL}/logs', json=transport_log.to_dict())
        #         transport_log_timestamp = transport_log_timestamp.json()['timestamp']
        #         print(transport_log_timestamp)
        #         # transport処理
        #         requests.patch(f'{LOG_SERVER_URL}/logs/{transport_log_timestamp}', json={'status': 'done', 'end_time': timestamp()})
        #         # self.update_log(timestamp, {'status': 'done'})
        #     # データ・ラボウェアのtransport

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
                log_timestamp = requests.post(f'{LOG_SERVER_URL}/logs', json=operation_log.to_dict())
                status = suit_machine.run()
                log_timestamp = log_timestamp.json()['timestamp']
                print(log_timestamp)
                requests.patch(f'{LOG_SERVER_URL}/logs/{log_timestamp}', json={'status': status, 'end_time': timestamp()})
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
                transport_log_timestamp = requests.post(f'{LOG_SERVER_URL}/logs', json=transport_log.to_dict())
                transport_log_timestamp = transport_log_timestamp.json()['timestamp']
                print(transport_log_timestamp)
                # transport処理
                requests.patch(f'{LOG_SERVER_URL}/logs/{transport_log_timestamp}', json={'status': 'done', 'end_time': timestamp()})
                # self.update_log(timestamp, {'status': 'done'})
            # データ・ラボウェアのtransport


async def read_uploaded_yaml(yaml_file: UploadFile = File(...)):
    if not yaml_file.filename.endswith(('.yaml', '.yml')):
        raise HTTPException(status_code=400, detail="Uploaded file must be a YAML file")
    try:
    # ファイルの内容を読み取る
        contents = await yaml_file.read()
        contents_str = contents.decode('utf-8')
        # yamlファイルを読み取る
        yaml_data = yaml.safe_load(StringIO(contents_str))
        return yaml_data
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def calc_md5_from_file(file: UploadFile = File(...)):
    file_content_bytes = await file.read()  # bytesで取得
    file_content_str = file_content_bytes.decode("utf-8")
    md5 = calculate_md5(file_content_str)
    await file.seek(0)
    return md5


@app.post("/run_experiment")
async def run_experiment(project_id: int, protocol_name, user_id: int, protocol_yaml: UploadFile = File(...), manipulate_yaml: UploadFile = File(...)):
    protocol_md5 = await calc_md5_from_file(protocol_yaml)
    protocol = await read_uploaded_yaml(protocol_yaml)
    manipulates = await read_uploaded_yaml(manipulate_yaml)
    storage_address = Path("/app/storage") / Path(timestamp_filename())
    machines = [
        HumanPlateServer("human_plate_server", manipulates, storage_address),
        TecanFluent480("tecan_fluent_480", manipulates, storage_address),
        OpentronsOT2("opentrons_ot2", manipulates, storage_address),
        TecanInfinite200Pro("tecan_infinite_200_pro", manipulates, storage_address),
        HumanStoreLabware("human_store_labware", manipulates, storage_address),
    ]

    response = requests.post(
        url=f'{LOG_SERVER_URL}/runs/',
        data={
            "project_id": project_id,
            "file_name": protocol_name,
            "checksum": protocol_md5,
            "user_id": user_id,
            "storage_address": storage_address
        }
    )
    run_id = response.json()["id"]
    # conductor = Conductor(
    #     project_id=project_id,
    #     protocol_name=protocol_name,
    #     protocol=protocol,
    #     checksum=protocol_md5,
    #     user_id=user_id,
    #     manipulates=manipulates,
    #     machines=machines,
    #     storage_address=storage_address
    # )
    # await conductor.register_operations()
    create_process_and_operation_and_edge(
        run_id=run_id,
        protocol_dict=protocol,
        machines=machines
    )
    # conductor.run()
    # return machines[0]


if __name__ == '__main__':
    demo_conductor = Conductor(
        project_id=1,
        protocol_name="test_protoco",
        protocol="poefihwpogiwofe",
        user_id=1,
        manipulates=[],
        machines=[],
        storage_address="test_storage_address"
    )
    # connections = Conductor.connections_from_protocol('protocol.yaml')
    # print(Conductor.create_plan(connections))