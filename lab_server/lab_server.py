from typing import List, Dict
from fastapi import FastAPI, File, UploadFile, HTTPException
from time import sleep
from random import uniform
from pathlib import Path
from log import OperationLog, TransportLog
from io import StringIO
import yaml
import requests

LAB_SERVER_URL = 'http://localhost:8000'

app = FastAPI()


class Operator:
    id: str
    type: str
    task_input: Dict[str, any]  # 型は何がいいだろうか？
    storage_address: str

    def __init__():
        pass

    def run(self):
        metadata_path = Path(self.storage_address) / Path('metadata.json')
        # ランダムな時間だけ待つ
        running_time = uniform(1, 10)
        sleep(running_time)
        # save metadata
        with open(metadata_path, 'w') as file:
            file.write('{"metadata": "sample_metadata"}')
        return "done"


class Conductor:
    def create_manipulate_dict(manipulate_yaml_path: str) -> Dict[str, Operator]:
        with open(manipulate_yaml_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
            return {data['name']: data for data in yaml_data}

    def create_operation_dict(protocol_yaml_path: str) -> Dict[str, List[str]]:
        with open(protocol_yaml_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
            operations = yaml_data['operations']
            return {operation['id']: operation for operation in operations}

    def connections_from_protocol(yaml_data: str) -> List[Dict[str, str]]:
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

    def update_log(id, data):
        requests.patch(f'{LAB_SERVER_URL}/logs/{id}', data)

    def run(self, experiment_plan: List[str], operator_dict: Dict[str, Operator]):
        for operator_name in experiment_plan:
            operator = operator_dict[operator_name]
            operation_log = OperationLog(

            )
            log_timestamp = requests.post(f'{LAB_SERVER_URL}/logs', data=operation_log.to_dict())
            status = operator.run()
            # requests.patch(f'{LAB_SERVER_URL}/logs/{log_timestamp}', data={'status': status})
            self.update_log(log_timestamp, {'status': status})
            activated_connection = [connection for connection in self.connections if connection['input_source'] == operator_name]
            for connection in activated_connection:
                transport_log = TransportLog()
                timestamp = requests.post(f'{LAB_SERVER_URL}/logs', data=transport_log.to_dict())
                # transport処理
                requests.patch(f'{LAB_SERVER_URL}/logs/{timestamp}', data={'status': 'done'})
                self.update_log(timestamp, {'status': 'done'})
            # データ・ラボウェアのtransport


@app.post("/run_experiment")
def run_experiment(protocol_yaml: UploadFile = File(...)):
    if not protocol_yaml.filename.endswith(('.yaml', '.yml')):
        raise HTTPException(status_code=400, detail="Uploaded file must be a YAML file")
    try:
        # ファイルの内容を読み取る
        contents = protocol_yaml.file.read()
        contents_str = contents.decode('utf-8')
        # yamlファイルを読み取る
        yaml_data = yaml.safe_load(StringIO(contents_str))
        connections = Conductor.connections_from_protocol(yaml_data)
        return Conductor.create_plan(connections)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


if __name__ == '__main__':
    connections = Conductor.connections_from_protocol('protocol.yaml')
    print(Conductor.create_plan(connections))
