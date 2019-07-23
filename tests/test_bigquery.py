from meta_qa.integrations.bigquery import BigQueryIntegration
from meta_qa.integrations.bigquery import BigQueryColumnOperators



GBQ_LOCATION = "`properati-data-public.properties_br.properties_rent_201501`"
GBQ_COLUMN = "id"

class TestBigQueryIntegration(object):


    def test_constructor(self):
        column_operator = BigQueryColumnOperators(GBQ_LOCATION, GBQ_COLUMN)
        assert column_operator is not None

    def test_nonnull(self):
        column_operator = BigQueryColumnOperators(GBQ_LOCATION, GBQ_COLUMN)
        assert column_operator.not_null().result is True
