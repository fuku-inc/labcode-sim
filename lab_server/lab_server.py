from typing import List, Dict, TypedDict
from datetime import datetime
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
from time import sleep
from random import uniform
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

    def run(self):

        self.started_at = datetime.now().isoformat()
        print(self.started_at)
        self.status = "running"
        requests.patch(
            url=f'{LOG_SERVER_URL}/operations/{self.db_id}',
            data={
                "attribute": "started_at",
                "new_value": self.started_at
            }
        )
        requests.patch(
            url=f'{LOG_SERVER_URL}/operations/{self.db_id}',
            data={
                "attribute": "status",
                "new_value": self.status
            }
        )
        running_time = uniform(1, 3)
        sleep(running_time)
        self.finished_at = datetime.now().isoformat()
        self.status = "completed"
        requests.patch(
            url=f'{LOG_SERVER_URL}/operations/{self.db_id}',
            data={
                "attribute": "finished_at",
                "new_value": self.finished_at
            }
        )
        requests.patch(
            url=f'{LOG_SERVER_URL}/operations/{self.db_id}',
            data={
                "attribute": "status",
                "new_value": self.status
            }
        )




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
                "name": self.id_in_protocol,
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
        if connection["is_data"]:
            edge_list.append({"from": operation_name_from, "to": operation_name_to})
        else:
            operation_list_from_connection.append(operation)
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
    # print(edge_list)
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
        # print(response.json())
    return operation_list, edge_list
    # print(edge_db_id_list)


def create_plan(connections: List[Dict[str, str]]) -> List[str]:
    """
    Create a plan from a protocol yaml file using a topological sort
    :param protocol_yaml_path: path to the protocol yaml file
    :return: a list of steps in the order they should
    """
    # make edge_list unique
    # edge_list = list(set(connections))
    edge_list = list(set([(connection['from'], connection['to']) for connection in connections]))
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
    operation_list, edge_list = create_process_and_operation_and_edge(
        run_id=run_id,
        protocol_dict=protocol,
        machines=machines
    )
    plan = create_plan(edge_list)
    run_start_time = datetime.now().isoformat()
    requests.patch(url=f'{LOG_SERVER_URL}/runs/{run_id}', data={"attribute": "started_at", "new_value": run_start_time})
    requests.patch(url=f'{LOG_SERVER_URL}/runs/{run_id}', data={"attribute": "status", "new_value": "running"})
    for operation_name in plan:
        operation = [operation for operation in operation_list if operation.name == operation_name][0]
        operation.run()
    run_finish_time = datetime.now().isoformat()
    requests.patch(url=f'{LOG_SERVER_URL}/runs/{run_id}', data={"attribute": "finished_at", "new_value": run_finish_time})
    requests.patch(url=f'{LOG_SERVER_URL}/runs/{run_id}', data={"attribute": "status", "new_value": "completed"})
