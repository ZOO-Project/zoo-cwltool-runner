import json
import os
import pathlib
import sys

import yaml
from dotenv import load_dotenv
from zoo_cwltool_runner import ExecutionHandler, ZooCwltoolRunner

load_dotenv()

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()


class CwltoolRunnerExecutionHandler(ExecutionHandler):
    def get_additional_parameters(self):
        return {
            "STAGEOUT_AWS_ACCESS_KEY_ID": os.getenv("AWS_SERVICE_URL", "pippo"),
            "STAGEOUT_AWS_SECRET_ACCESS_KEY": os.getenv("AWS_REGION", "pippo"),
            "STAGEOUT_AWS_REGION": os.getenv("AWS_ACCESS_KEY_ID", "pippo"),
            "STAGEOUT_AWS_SERVICEURL": os.getenv("AWS_SECRET_ACCESS_KEY", "pippo"),
            "STAGEOUT_OUTPUT": "aaa",
            "process": "water_bodies",
        }

    def handle_outputs(self, log, output, usage_report, tool_logs):
        os.makedirs(
            os.path.join(self.conf["tmpPath"], self.job_id),
            mode=0o777,
            exist_ok=True,
        )
        with open(os.path.join(self.conf["tmpPath"], self.job_id, "job.log"), "w") as f:
            f.writelines(log)

        with open(os.path.join(self.conf["tmpPath"], self.job_id, "output.json"), "w") as output_file:
            json.dump(output, output_file, indent=4)

        print(self.conf, sys.stderr)


def water_bodies(conf, inputs, outputs):
    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        cwl = yaml.safe_load(stream)

    runner = ZooCwltoolRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=CwltoolRunnerExecutionHandler(conf=conf),
    )
    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        outputs = runner.outputs
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
