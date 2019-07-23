import pandas as pd
from meta_qa import QAResult

class BaseIntegration:
    """
    Interface for implementing integrations.
    """
    def __init__(self):
        pass


    def get_metadata(self) -> pd.DataFrame:
        """
        Gets an table containing all relevant metadata on an dataset.
        """
        pass


class BaseColumnOperators:

    def __init__(self, table_name, column_name):
        pass

    def lower_than(self, other_column: str) -> QAResult:
        pass


    def larger_than(self, other_column: str) -> QAResult:
        pass


    def not_null(self, tol: int=0) -> QAResult:
        pass


    def not_duplicates(self) -> QAResult:
        pass


    def not_percentile_null(self, q: float=0.05) -> QAResult:
        pass


    def not_window_percentile_null(self,
                                   q: float=0.05,
                                   days: float=90) -> QAResult:
        pass


    def timestamp_sanity(self) -> QAResult:
        pass


    def related_to(self, other_table: str, other_column: str='') -> QAResult:
        pass


    def formated_as(self, format: str) -> QAResult:
        pass


    def list_related_to(self,
                        other_table: str,
                        other_column: str='') -> QAResult:
        pass

    def between(self, lower_bound: float, upper_bound: float) -> QAResult:
        pass
