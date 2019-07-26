from types import SimpleNamespace

import pandas as pd

from meta_qa import QAResult
from meta_qa.parser import parse_task, parse_text


class BaseIntegration:
    """
    Interface for implementing integrations.
    """

    def __init__(self, params: dict):
        self.column_operators = BaseColumnOperators
        self.params = {}
        pass

    def get_metadata(self) -> pd.DataFrame:
        """
        Gets an table containing all relevant metadata on an dataset.
        """
        pass

    def get_tasks(self, metadata: pd.DataFrame) -> pd.DataFrame:
        return metadata["column_assert"].apply(pd.Series).applymap(parse_task)

    def lambda_task(self, df):
        location = "`{}.{}.{}`".format(df["table_catalog"],
                                       df["table_schema"],
                                       df["table_name"])

        column_params = {"table_name": location,
                         "column_name": df["column_name"]}
        column_operator = self.column_operators(column_params)

        column_output = []

        for task in df["tasks"]:
            operation_result = column_operator.function(task)
            column_output.append(operation_result)
        return column_output

    def run_pipeline(self) -> dict:

        # Get metadata
        metadata = self.get_metadata().reset_index()
        tasks = self.get_tasks(metadata).apply(list, axis=1)

        metadata_with_tasks = metadata.assign(tasks=tasks).sample(5)

        print(metadata_with_tasks.head(5))

        result = metadata_with_tasks.apply(self.lambda_task, axis=1)

        tasks_output = {"num_tasks": len(tasks),
                        "result": result}

        # Run operators

        return SimpleNamespace(**tasks_output)


class BaseColumnOperators:

    def __init__(self, column_params: dict):
        self.params = column_params
        self.table_name = column_params["table_name"]
        self.column_name = column_params["column_name"]

    def qa_result(self, result, raw_result, operator):
        output = {"result": result,
                  "raw_result": raw_result,
                  "operator": operator,
                  **self.column_params}
        return output

    def function(self, params: tuple):
        if params is None:
            return None
        function_name = params[0]
        pos_args = params[1]["positional_args"]
        kw_args = params[1]["keyword_args"]
        function = task_function = getattr(self, function_name)
        try:
            result = function(*pos_args, **kw_args)
        except Exception as error:
            result = self.qa_result(False, error, function_name)
            print("Error: {} \n".format(result))
        finally:
            return result

    def lower_than(self, other_column: str) -> QAResult:
        pass

    def larger_than(self, other_column: str) -> QAResult:
        pass

    def not_null(self, tol: int = 0) -> QAResult:
        pass

    def not_duplicates(self) -> QAResult:
        pass

    def not_percentile_null(self, q: float = 0.05) -> QAResult:
        pass

    def not_window_percentile_null(self,
                                   q: float = 0.05,
                                   days: float = 90) -> QAResult:
        pass

    def timestamp_sanity(self) -> QAResult:
        pass

    def related_to(self, other_table: str, other_column: str = '') -> QAResult:
        pass

    def formated_as(self, format: str) -> QAResult:
        pass

    def list_related_to(self,
                        other_table: str,
                        other_column: str = '') -> QAResult:
        pass

    def between(self, lower_bound: float, upper_bound: float) -> QAResult:
        pass
