from meta_qa.integrations.bigquery import (BigQueryColumnOperators,
                                           BigQueryIntegration)

GBQ_LOCATION = "`idwall-data.dw_idwall.ft_attempts`"
GBQ_COLUMN = "id_attempt"


class TestBigQueryIntegration(object):

    """
    def test_constructor(self):
        column_operator = BigQueryColumnOperators(GBQ_LOCATION, GBQ_COLUMN)
        assert column_operator is not None

    def test_nonnull(self):
        column_operator = BigQueryColumnOperators(GBQ_LOCATION, GBQ_COLUMN)
        assert column_operator.not_null().result == True

    """

    def test_operator(self):
        input = ("not_null", {"positional_args": [], "keyword_args": {}})
        params = {"table_name": GBQ_LOCATION,
                  "column_name": GBQ_COLUMN}
        column_operator = BigQueryColumnOperators(params)
        assert ((column_operator.function(input)).result == True)

    def test_pipeline(self):
        bigquery_integration = BigQueryIntegration("idwall-data", "dw_idwall")
        pipeline_result = bigquery_integration.run_pipeline()
        assert (pipeline_result.num_tasks > 0)
