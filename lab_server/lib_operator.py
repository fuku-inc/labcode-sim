from typing import List
from time import sleep
from random import uniform
from pathlib import Path


class Operator:
    id: str
    type: str
    task_input: List[str]
    task_output: List[str]
    storage_address: Path

    def __init__(self, id, type, manipulate_list, storage_address):
        self.id = id
        self.type = type
        self.storage_address = storage_address / Path(id)
        # 該当するmanipulateが1つしかないことを想定している。
        manipulate = [manipulate for manipulate in manipulate_list if manipulate['name'] == type][0]
        if manipulate.get('input'):
            self.task_input = [input['id'] for input in manipulate['input']]
        if manipulate.get('output'):
            self.task_output = [output['id'] for output in manipulate['output']]

    def run(self):
        print(self.storage_address)
        Path(self.storage_address).mkdir(parents=True)
        metadata_path = Path(self.storage_address) / Path('metadata.json')
        # ランダムな時間だけ待つ
        running_time = uniform(1, 3)
        sleep(running_time)
        # save metadata
        with open(metadata_path, 'w') as file:
            file.write('{"metadata": "sample_metadata"}')
        return "done"
