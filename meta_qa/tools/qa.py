import pandas as pd
import numpy as np
from pandarallel import pandarallel

def apply_operators(row, integration):
    """
    Applies the associated column functions for an given database column
    """
    project_id = integration.params["project_id"]
    dataset = integration.params["dataset"]
    table_name = row.name[-2]
    column_name = row.name[-1]
    operator = integration.column_operators(project_id,
                                            dataset,
                                            table_name,
                                            column_name)
    return row.map(operator.function)


def run_qa_operators(tasks, integration, n_workers=None):
    """
    Apply the QA operators on an tasks dataset, from self.get_tasks.
    """
    # Apply the QA functions associated with each column
    pandarallel.initialize(nb_workers=n_workers)
    pipeline_out = tasks.parallel_apply(apply_operators,
                                        args=[integration],
                                        axis=1)

    # Wrangle the data for having something more intuitive
    pipeline_stack = pipeline_out.stack().droplevel(-1)
    operators = pipeline_stack.apply(lambda x: x["operator"])
    unique_ops = operators.values.flatten()
    unique_ops = unique_ops[unique_ops != None]
    unique_ops = np.unique(unique_ops)
    output = []
    for operator in operators.unique():
        operator_results = (pd.DataFrame(pipeline_stack.where(operators == operator)
                                         .dropna()).assign(operator=operator)
                            .set_index("operator", append=True)
                            .rename(columns={0: "result_object"})
                            .pipe(lambda df: df.loc[~df.index.duplicated()]) # GAMBIARRA
                            .unstack())
        operator_results.columns = [operator]
        output.append(operator_results)
    pipeline_result = pd.concat(output, sort=False, axis=1)
    return pipeline_result


def beautify_pipeline_cell(cell):
    """
    Transforms the QA pipeline elements into something more human readable.
    """
    nice_result = ""
    if type(cell) is dict:
        raw_result = cell["raw_result"]
        if issubclass(type(raw_result), Exception):
            parenthesis = "error"
        elif type(raw_result) is float:
            parenthesis = "{:.2f}".format(raw_result)
        else:
            parenthesis = raw_result
        nice_result = "{} ({})".format(cell["result"], parenthesis)
    return nice_result


def run_qa_pipeline(integration, n_workers=None):
    metadata = integration.get_metadata()
    tasks = integration.get_tasks(metadata)
    pipeline_result = run_qa_operators(tasks, integration,
                                       n_workers=n_workers)
    return pipeline_result.applymap(beautify_pipeline_cell)
