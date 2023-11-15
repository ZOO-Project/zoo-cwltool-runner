import argparse
import inspect
import json
import os
import uuid
from datetime import datetime
from io import StringIO
from shutil import which
from typing import Union

import attr
import cwl_utils
import yaml
from cwl_utils.parser import load_document_by_yaml
from cwl_wrapper.parser import Parser
from cwltool.main import main
from loguru import logger
from zoo_cwltool_runner.handlers import ExecutionHandler


# useful class for hints in CWL
@attr.s
class ResourceRequirement:
    coresMin = attr.ib(default=None)
    coresMax = attr.ib(default=None)
    ramMin = attr.ib(default=None)
    ramMax = attr.ib(default=None)
    tmpdirMin = attr.ib(default=None)
    tmpdirMax = attr.ib(default=None)
    outdirMin = attr.ib(default=None)
    outdirMax = attr.ib(default=None)

    @classmethod
    def from_dict(cls, env):
        return cls(**{k: v for k, v in env.items() if k in inspect.signature(cls).parameters})


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


class Workflow:
    def __init__(self, cwl, workflow_id):
        self.raw_cwl = cwl
        self.cwl = load_document_by_yaml(yaml=cwl, id_=workflow_id, uri="io://")
        self.workflow_id = workflow_id

    def get_workflow(self) -> cwl_utils.parser.cwl_v1_0.Workflow:
        return self.cwl

    def get_object_by_id(self, id):
        ids = [elem.id.split("#")[-1] for elem in self.cwl]
        return self.cwl[ids.index(id)]

    def get_workflow_inputs(self, mandatory=False):
        inputs = []
        for inp in self.get_workflow().inputs:
            if mandatory:
                if inp.default is not None or inp.type == ["null", "string"]:
                    continue
                else:
                    inputs.append(inp.id.split("/")[-1])
            else:
                inputs.append(inp.id.split("/")[-1])
        return inputs

    @staticmethod
    def has_scatter_requirement(workflow):
        return any(
            isinstance(
                requirement,
                (
                    cwl_utils.parser.cwl_v1_0.ScatterFeatureRequirement,
                    cwl_utils.parser.cwl_v1_1.ScatterFeatureRequirement,
                    cwl_utils.parser.cwl_v1_2.ScatterFeatureRequirement,
                ),
            )
            for requirement in workflow.requirements
        )

    @staticmethod
    def get_resource_requirement(elem):
        """Gets the ResourceRequirement out of a CommandLineTool or Workflow

        Args:
            elem (CommandLineTool or Workflow): CommandLineTool or Workflow

        Returns:
            cwl_utils.parser.cwl_v1_2.ResourceRequirement or ResourceRequirement
        """
        resource_requirement = [
            requirement
            for requirement in elem.requirements
            if isinstance(
                requirement,
                (
                    cwl_utils.parser.cwl_v1_0.ResourceRequirement,
                    cwl_utils.parser.cwl_v1_1.ResourceRequirement,
                    cwl_utils.parser.cwl_v1_2.ResourceRequirement,
                ),
            )
        ]

        if len(resource_requirement) == 1:
            return resource_requirement[0]

        # look for hints
        if elem.hints is not None:
            resource_requirement = [
                ResourceRequirement.from_dict(hint)
                for hint in elem.hints
                if hint["class"] == "ResourceRequirement"
            ]

        if len(resource_requirement) == 1:
            return resource_requirement[0]

    def eval_resource(self):
        resources = {
            "coresMin": [],
            "coresMax": [],
            "ramMin": [],
            "ramMax": [],
            "tmpdirMin": [],
            "tmpdirMax": [],
            "outdirMin": [],
            "outdirMax": [],
        }

        for elem in self.cwl:
            if isinstance(
                elem,
                (
                    cwl_utils.parser.cwl_v1_0.Workflow,
                    cwl_utils.parser.cwl_v1_1.Workflow,
                    cwl_utils.parser.cwl_v1_2.Workflow,
                ),
            ):
                if resource_requirement := self.get_resource_requirement(elem):
                    for resource_type in [
                        "coresMin",
                        "coresMax",
                        "ramMin",
                        "ramMax",
                        "tmpdirMin",
                        "tmpdirMax",
                        "outdirMin",
                        "outdirMax",
                    ]:
                        if getattr(resource_requirement, resource_type):
                            resources[resource_type].append(getattr(resource_requirement, resource_type))
                for step in elem.steps:
                    if resource_requirement := self.get_resource_requirement(
                        self.get_object_by_id(step.run[1:])
                    ):
                        multiplier = 2 if step.scatter else 1
                        for resource_type in [
                            "coresMin",
                            "coresMax",
                            "ramMin",
                            "ramMax",
                            "tmpdirMin",
                            "tmpdirMax",
                            "outdirMin",
                            "outdirMax",
                        ]:
                            if getattr(resource_requirement, resource_type):
                                resources[resource_type].append(
                                    getattr(resource_requirement, resource_type) * multiplier
                                )
        return resources


class ZooConf:
    def __init__(self, conf):
        self.conf = conf
        self.workflow_id = self.conf["lenv"]["Identifier"]


class ZooInputs:
    def __init__(self, inputs):
        self.inputs = inputs

    def get_input_value(self, key):
        try:
            return self.inputs[key]["value"]
        except KeyError as exc:
            raise exc
        except TypeError:
            pass

    def get_processing_parameters(self):
        """Returns a list with the input parameters keys"""
        return {key: value["value"] for key, value in self.inputs.items()}


class ZooOutputs:
    def __init__(self, outputs):
        self.outputs = outputs

    def get_output_parameters(self):
        """Returns a list with the output parameters keys"""
        return {key: value["value"] for key, value in self.outputs.items()}

    def set_output(self, value):
        """set the output result value"""
        if "stac" in self.outputs.keys():
            self.outputs["stac"]["value"] = value
        else:
            self.outputs["stac"] = {"value": value}


class ZooCwltoolRunner:
    def __init__(
        self,
        cwl,
        conf,
        inputs,
        outputs,
        execution_handler: Union[ExecutionHandler, None] = None,
    ):
        self.zoo_conf = ZooConf(conf)
        self.inputs = ZooInputs(inputs)
        self.outputs = ZooOutputs(outputs)
        self.cwl = Workflow(cwl, self.zoo_conf.workflow_id)

        self.handler = execution_handler

        self.storage_class = os.environ.get("STORAGE_CLASS", "openebs-nfs-test")
        self.monitor_interval = 30
        self._namespace_name = None

        if which("podman"):
            self.podman = True
        elif which("docker"):
            self.podman = False
        else:
            raise ValueError("No container engine")

    def update_status(self, progress: int, message: str = None) -> None:
        """updates the execution progress (%) and provides an optional message"""
        if message:
            self.zoo_conf.conf["lenv"]["message"] = message

        zoo.update_status(self.zoo_conf.conf, progress)

    def get_workflow_id(self):
        """returns the workflow id (CWL entry point)"""
        return self.zoo_conf.workflow_id

    def get_processing_parameters(self):
        """Gets the processing parameters from the zoo inputs"""
        return self.inputs.get_processing_parameters()

    def get_workflow_inputs(self, mandatory=False):
        """Returns the CWL workflow inputs"""
        return self.cwl.get_workflow_inputs(mandatory=mandatory)

    def assert_parameters(self):
        """checks all mandatory processing parameters were provided"""
        return all(
            elem in list(self.get_processing_parameters().keys())
            for elem in self.get_workflow_inputs(mandatory=True)
        )

    @staticmethod
    def shorten_namespace(value: str) -> str:
        """shortens the namespace to 63 characters"""
        while len(value) > 63:
            value = value[:-1]
            while value.endswith("-"):
                value = value[:-1]
        return value

    def get_job_id(self):
        """creates or returns the namespace"""
        return self.shorten_namespace(
            f"{str(self.zoo_conf.workflow_id).replace('_', '-')}-"
            f"{str(datetime.now().timestamp()).replace('.', '')}-{uuid.uuid4()}"
        )

    def execute(self):
        if not (self.assert_parameters()):
            logger.error("Mandatory parameters missing")
            return zoo.SERVICE_FAILED

        logger.info("execution started")
        self.update_status(progress=2, message="starting execution")

        logger.info("wrap CWL workflow with stage-in/out steps")
        wrapped_workflow = self.wrap()
        self.update_status(progress=5, message="workflow wrapped")

        logger.info("run the CWL workflow")

        processing_parameters = {
            **self.get_processing_parameters(),
            **self.handler.get_additional_parameters(),
        }

        self.handler.set_job_id(job_id=self.get_job_id())
        with open("temp-app-package.cwl", "w") as file:
            print(wrapped_workflow, file=file)

        with open("params.yaml", "w") as file:
            print(yaml.dump(processing_parameters), file=file)

        self.update_status(progress=18, message="execution submitted")

        logger.info("execution")

        parsed_args = argparse.Namespace(
            podman=self.podman,
            parallel=True,
            debug=False,
            outdir="./runs",
            workflow="temp-app-package.cwl",
            job_order=["params.yaml"],
        )

        stream_out = StringIO()
        stream_err = StringIO()

        res = main(
            args=parsed_args,
            stdout=stream_out,
            stderr=stream_err,
        )

        if res == 0:
            logger.info("execution complete")
            exit_value = zoo.SERVICE_SUCCEEDED
        else:
            logger.error("execution failed")
            exit_value = zoo.SERVICE_FAILED

        self.update_status(progress=90, message="delivering outputs, logs and usage report")

        logger.info("handle outputs execution logs")
        output = json.loads(stream_out.getvalue())
        self.outputs.set_output(output)

        self.handler.handle_outputs(
            log=stream_err.getvalue(),
            output=output,
            usage_report=None,
            tool_logs=None,
        )

        self.update_status(progress=97, message="clean-up resources")

        logger.info("clean-up resources")
        os.remove("temp-app-package.cwl")
        os.remove("params.yaml")

        self.update_status(
            progress=100,
            message=f'execution {"failed" if exit_value == zoo.SERVICE_FAILED else "successful"}',
        )

        return exit_value

    def wrap(self):
        workflow_id = self.get_workflow_id()

        os.environ["WRAPPER_STAGE_IN"] = "/data/work/zoo/zoo-cwltool-runner/assets/stagein.yaml"
        os.environ["WRAPPER_STAGE_OUT"] = "/data/work/zoo/zoo-cwltool-runner/assets/stageout.yaml"
        os.environ["WRAPPER_MAIN"] = "/data/work/zoo/zoo-cwltool-runner/assets/main.yaml"
        os.environ["WRAPPER_RULES"] = "/data/work/zoo/zoo-cwltool-runner/assets/rules.yaml"

        wf = Parser(
            cwl=self.cwl.raw_cwl,
            output=None,
            stagein=os.environ.get("WRAPPER_STAGE_IN", "/assets/stagein.yaml"),
            stageout=os.environ.get("WRAPPER_STAGE_OUT", "/assets/stageout.yaml"),
            maincwl=os.environ.get("WRAPPER_MAIN", "/assets/maincwl.yaml"),
            rulez=os.environ.get("WRAPPER_RULES", "/assets/rules.yaml"),
            assets=None,
            workflow_id=workflow_id,
        )

        return wf.out
