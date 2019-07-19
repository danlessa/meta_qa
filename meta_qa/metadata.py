# -*- coding: utf-8 -*-

from integrations import BaseIntegration


class Metadata:
    """

    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


    def run_pipeline(self, integration: BaseIntegration, pipeline=[]: list):
        if ((pipeline is None) or (len(pipeline) == 0)):
            pipeline = self.pipeline
        pipeline_output = []
        for task in pipeline:
            task_result = self.run_qa_operator(task, integration)
            pipeline_output.append(task_result)
        return pipeline_output


    def run_qa_operator(self, task:str, integration: BaseIntegration):
        (raw_function, params) = parser.parse_task(task, integration)
        function = getattr(raw_function, integration)
        task_result = function(*params)
        return task_result
