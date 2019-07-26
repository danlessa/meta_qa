import datetime
from types import SimpleNamespace

import pandas as pd

from meta_qa.integrations.base import BaseColumnOperators, BaseIntegration
from meta_qa.parser import parse_task, parse_text


class BigQueryIntegration(BaseIntegration):
    """
    Implementation
    """

    def __init__(self, project_id, dataset):
        self.column_operators = BigQueryColumnOperators
        self.params = {"project_id": project_id,
                       "dataset": dataset}

    def load_table_metadata(self) -> pd.DataFrame:
        query = """
        SELECT  table_catalog,
                table_schema,
                table_name,
                option_value
        FROM {}.INFORMATION_SCHEMA.TABLE_OPTIONS as tables
        WHERE option_name = 'description'
        """.format(self.params["dataset"])

        table_metadata = pd.read_gbq(
            query, project_id=self.params["project_id"], dialect='standard')

        def parse_f(df): return df.join(df["option_value"].map(parse_text)
                                        .apply(pd.Series))

        def ignore_f(df): return df.assign(
            ignore=df["ignore"].where(~pd.isnull(df["ignore"]), 0))
        table_metadata = (table_metadata.pipe(parse_f)
                                        .drop(columns=["option_value"])
                                        .rename(columns={"option_type": "column_type",
                                                         "option_name": "column_name"})
                                        .set_index(["table_catalog",
                                                    "table_schema",
                                                    "table_name"])
                                        .pipe(ignore_f))
        return table_metadata

    def load_column_metadata(self) -> pd.DataFrame:
        query = """
        SELECT table_catalog,
               table_schema,
               table_name,
               field_path AS column_name,
               data_type,
               description
        FROM {}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS as tables
        """.format(self.params["dataset"])

        columns_metadata = (pd.read_gbq(query,
                                        project_id=self.params["project_id"],
                                        dialect='standard')
                            .set_index(["table_catalog",
                                        "table_schema",
                                        "table_name",
                                        "column_name"]))
        cols = ["data_type", "description"]

        def parse_f(df): return df.join(df["description"].map(parse_text)
                                        .apply(pd.Series))

        columns_variables = (columns_metadata.loc[:, cols]
                                             .pipe(parse_f)
                                             .drop(columns=["description"])
                                             .fillna(''))
        return columns_variables

    def get_metadata(self) -> pd.DataFrame:
        table_metadata = self.load_table_metadata()
        columns_metadata = self.load_column_metadata()
        metadata = (table_metadata.add_prefix("table_")
                    .join(columns_metadata.add_prefix("column_")))
        return metadata


class BigQueryColumnOperators(BaseColumnOperators):

    def __init__(self, project_id, dataset, table_name, column_name):
        column_params = {"project_id": project_id,
                         "dataset": dataset,
                         "table_name": table_name,
                         "column_name": column_name}
        self.column_params = column_params
        self.project_id = project_id
        self.dataset = dataset
        self.table_name = table_name
        self.column_name = column_name

    def table_location(self, table_name=""):
        if table_name == "":
            table_name = self.table_name
        return "`{}.{}.{}`".format(self.project_id,
                                   self.dataset,
                                   table_name)

    def lower_than(self, other_column_name: str) -> dict:
        params = {"table": self.table_location(),
                  "self_column": self.column_name,
                  "other_column":  other_column_name}
        query = """
        SELECT SUM(IF({self_column} > {other_column}, 1, 0))
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 0)
        return self.qa_result(condition, query_result, 'lower_than')

    def larger_than(self, other_column_name: str) -> dict:
        params = {"table": self.table_location(),
                  "self_column": self.column_name,
                  "other_column":  other_column_name}
        query = """
        SELECT SUM(IF({self_column} < {other_column}, 1, 0))
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 0)
        return self.qa_result(condition, query_result, 'larger_than')

    def not_null(self) -> dict:
        params = {"table": self.table_location(),
                  "self_column": self.column_name}
        query = """
        SELECT COUNT({self_column}) / COUNT(*)
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'not_null')

    def not_duplicates(self) -> dict:
        params = {"table": self.table_location(),
                  "self_column": self.column_name}
        query = """
        SELECT COUNT(DISTINCT({self_column})) / COUNT({self_column})
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'not_duplicates')

    def not_percentile_null(self, q: float = 0.05) -> dict:
        params = {"table": self.table_location(),
                  "self_column": self.column_name}
        query = """
        SELECT COUNT({self_column}) / COUNT(*)
        FROM {table}
        """.format(**params)
        query_result = (
            1 - pd.read_gbq(query, dialect='standard').values[0][0])
        condition = (query_result < q)
        return self.qa_result(condition, query_result, 'not_percentile_null')

    def not_window_percentile_null(self, on: str = '',
                                   days: float = 90, q: float = 0.05) -> dict:
        """
        Checks if an given column has an certain quantity of nulls inside
        an temporal windows of days until now.
        """

        if on == '':
            on = self.column_name

        now = datetime.datetime.today()
        date_end = now.strftime('%Y-%m-%d')
        date_start = (now - datetime.timedelta(days=days)).strftime('%Y-%m-%d')

        params = {"table": self.table_location(),
                  "self_column": self.column_name,
                  "on": on,
                  "date_start": date_start,
                  "date_end": date_end}

        query = """
        SELECT COUNT({self_column}) / COUNT(*)
        FROM {table}
        WHERE {on} < '{date_end}'
        AND {on} > '{date_start}'
        """.format(**params)

        query_result = (
            1 - pd.read_gbq(query, dialect='standard').values[0][0])

        condition = (query_result < q)

        return self.qa_result(condition, query_result, 'not_window_percentile_null')

    def timestamp_sanity(self) -> dict:
        now = datetime.datetime.today()
        date_end = (now + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        params = {"table": self.table_location(),
                  "self_column": self.column_name,
                  "sanity_start": '2016-01-01',
                  "sanity_end": date_end}
        query = """
        SELECT COUNT(*)
        FROM {table}
        WHERE {self_column} < '{sanity_start}'
        OR {self_column} > '{sanity_end}'
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 0)
        return self.qa_result(condition, query_result, 'timestamp_sanity')

    def related_to(self, other_table: str = "", other_column: str = "") -> dict:
        if other_column == "":
            other_column = self.column_name
        if other_table == "":
            convert = lambda x: "lk_" + "_".join(x.split("_")[1:])
            other_table = convert(other_table)
        params = {"table": self.table_location(),
                  "self_column": self.column_name,
                  "other_column": other_column,
                  "other_table": self.table_location(other_table)}
        query = """
        SELECT COUNT(DISTINCT(t1.{self_column})) / COUNT(DISTINCT(t2.{other_column}))
        FROM {table} AS t1
        LEFT JOIN {other_table} as t2 on t1.{self_column} = t2.{other_column}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'related_to')


    def check_stage(self, stage_location, stage_column):
        params = {"stage_location": "stage.stg_" + stage_location,
                  "stage_column": stage_column,
                  "dw_location": self.table_location(),
                  "dw_column": self.column_name}

        query = """
        SELECT  COUNT(t1.{stage_column})
        FROM {stage_location} t1
        LEFT OUTER JOIN {dw_location} AS t2
        ON (t1.{stage_column} = t2.{dw_column})
        WHERE t2.{dw_column} IS NULL
        """.format(**params)

        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 0.0)
        return self.qa_result(condition, query_result, 'check_stage')
