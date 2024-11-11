class Log:
    start_time: str
    end_time: str
    status: str
    user_id: str
    lab_id: str
    protocol_id: str
    is_transport: bool
    operator_id: str
    execution_id: str
    storage_address: str

    def __init__(
            self,
            start_time,
            status,
            user_id,
            lab_id,
            protocol_id,
            is_transport,
            operator_id,
            execution_id,
            storage_address
    ):
        self.start_time = start_time
        self.end_time = ""
        self.status = status
        self.user_id = user_id
        self.lab_id = lab_id
        self.protocol_id = protocol_id
        self.is_transport = is_transport
        self.operator_id = operator_id
        self.execution_id = execution_id
        self.storage_address = storage_address


class OperationLog(Log):
    is_transport = False
    task_id: str

    def __init__(
            self,
            start_time,
            status,
            user_id,
            lab_id,
            protocol_id,
            task_id,
            operator_id,
            execution_id,
            storage_address
    ):
        super().__init__(
            status,
            start_time,
            user_id,
            lab_id,
            protocol_id,
            is_transport=False,
            operator_id=operator_id,
            execution_id=execution_id,
            storage_address=storage_address
        )
        self.task_id = task_id

    def to_dict(self):
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'status': self.status,
            'user_id': self.user_id,
            'lab_id': self.lab_id,
            'protocol_id': self.protocol_id,
            'is_transport': self.is_transport,
            'task_id': self.task_id,
            'operator_id': self.operator_id,
            'execution_id': self.execution_id,
            'storage_address': self.storage_address,
        }


class TransportLog(Log):
    is_transport = True
    source_task_id: str
    source_port_id: str
    destination_task_id: str
    destination_port_id: str

    def __init__(
            self,
            start_time,
            status,
            user_id,
            lab_id,
            protocol_id,
            source_task_id,
            source_port_id,
            destination_task_id,
            destination_port_id,
            operator_id,
            execution_id,
            storage_address
    ):
        super().__init__(
            status,
            start_time,
            user_id,
            lab_id,
            protocol_id,
            is_transport=True,
            operator_id=operator_id,
            execution_id=execution_id,
            storage_address=storage_address
        )
        self.source_task_id = source_task_id
        self.source_port_id = source_port_id
        self.destination_task_id = destination_task_id
        self.destination_port_id = destination_port_id

    def to_dict(self):
        return {
            'start_time': self.start_time,
            'end_time': self.end_time,
            'status': self.status,
            'user_id': self.user_id,
            'lab_id': self.lab_id,
            'protocol_id': self.protocol_id,
            'is_transport': self.is_transport,
            'source_task_id': self.source_task_id,
            'source_port_id': self.source_port_id,
            'destination_task_id': self.destination_task_id,
            'destination_port_id': self.destination_port_id,
            'operator_id': self.operator_id,
            'execution_id': self.execution_id,
            'storage_address': self.storage_address
        }
