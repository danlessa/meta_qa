import pandas as pd
from types import SimpleNamespace

from meta_qa.integrations.base import BaseIntegration
from meta_qa.integrations.base import BaseColumnOperators


class BigQueryIntegration(BaseIntegration):
    """
    Implementation
    """
    def __init__(self):
        pass

class BigQueryColumnOperators(BaseColumnOperators):

    def __init__(self, table: str, column_name: str):
        self.table = table
        self.column_name = column_name


    def qa_result(self, result, raw_result, operator):
        output = {"location": self.table,
                  "column_name": self.column_name,
                  "result": result,
                  "raw_result": raw_result,
                  "operator": operator}
        return SimpleNamespace(**output)


    def lower_than(self, other_column_name: str) -> dict:
        params = {"table": self.table,
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
        params = {"table": self.table,
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
        params = {"table": self.table,
                  "self_column": self.column_name}
        query = """
        SELECT COUNT({self_column}) / COUNT(*)
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'not_null')


    def not_duplicates(self) -> dict:
        params = {"table": self.table,
                  "self_column": self.column_name}
        query = """
        SELECT COUNT(DISTINCT({self_column})) / COUNT({self_column})
        FROM {table}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'not_duplicates')


    def not_percentile_null(self, q: float = 0.05) -> dict:
        params = {"table": self.table,
                  "self_column": self.column_name}
        query = """
        SELECT COUNT({self_column}) / COUNT(*)
        FROM {table}
        """.format(**params)
        query_result = (1 - pd.read_gbq(query, dialect='standard').values[0][0])
        condition = (query_result < q)
        return self.qa_result(condition, query_result, 'not_percentile_null')


    def not_window_percentile_null(self, on: str = '',
                                   days: float = 90, q: float = 0.05) -> dict:

        if on == '':
            on = self.column_name

        now = datetime.datetime.today()
        date_end = now.strftime('%Y-%m-%d')
        date_start = (now - datetime.timedelta(days=days)).strftime('%Y-%m-%d')

        params = {"table": self.table,
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
        query_result = (1 - pd.read_gbq(query, dialect='standard').values[0][0])
        condition = (query_result < q)
        return self.qa_result(condition, query_result, 'not_window_percentile_null')


    def timestamp_sanity(self) -> dict:
        now = datetime.datetime.today()
        date_end = (now + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        params = {"table": self.table,
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


    def related_to(self, other_table: str, other_column: str = "") -> dict:
        if other_column == "":
            other_column = self.column_name
        params = {"table": self.table,
                  "self_column": self.column_name,
                  "other_column": other_column,
                  "other_table": other_table}
        query = """
        SELECT COUNT(DISTINCT(t1.{self_column})) / COUNT(DISTINCT(t2.{other_column}))
        FROM {table} AS t1
        LEFT JOIN {other_table} as t2 on t1.{self_column} = t2.{other_column}
        """.format(**params)
        query_result = pd.read_gbq(query, dialect='standard').values[0][0]
        condition = (query_result == 1.0)
        return self.qa_result(condition, query_result, 'related_to')
