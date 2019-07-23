import pandas as pd

def valid_pipeline(df: pd.Series) -> pd.Series:
    """
    Checks if an given metadata pipelne is valid
    """
    table = "{}.{}".format(df["table_schema"], df["table_name"])
    column_name = df["column_name"]
    try:
        pipeline = parse_functions(df["column_assert"])
        flag = True
    except:
        pipeline = None
        flag = False
    finally:
        output = {"args": [table, column_name, pipeline], "flag": flag}
        return pd.Series(output)


def qa_process(df: pd.Series) -> pd.Series:
    """
    .
    """
    validation = valid_pipeline(df)
    if (validation.flag == True):
        column_qa = ColumnQA(*validation.args)
        output = column_qa.run_pipeline()
    else:
        output = None
    return pd.Series({"pipeline_result": output})
