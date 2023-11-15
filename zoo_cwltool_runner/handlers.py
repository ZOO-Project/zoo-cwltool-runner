from abc import ABC, abstractmethod


class ExecutionHandler(ABC):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.job_id = None

    def set_job_id(self, job_id):
        self.job_id = job_id

    @abstractmethod
    def handle_outputs(self, execution_log, output, usage_report, tool_logs=None):
        pass

    @abstractmethod
    def get_additional_parameters(self):
        pass
