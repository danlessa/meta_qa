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


    def lower_than(self) -> QAResult:
        pass


    def larger_than(self) -> QAResult:
        pass


    def not_null(self) -> QAResult:
        pass


    def not_duplicates(self) -> QAResult:
        pass


    def not_percentile_null(self) -> QAResult:
        pass


    def not_window_percentile_null(self, q=0.05: float, days=90: float) -> QAResut:
        pass


    def timestamp_sanity(self):
        pass


    def related_to(self):
        pass
