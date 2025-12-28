from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        tests=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        self.log.info("DataQualityOperator started")

        if not self.tests:
            raise ValueError("No data quality tests provided to DataQualityOperator")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, test in enumerate(self.tests, start=1):
            sql = test["sql"]
            expected = test["expected_result"]

            self.log.info("Running test %s: %s", i, sql)
            records = redshift.get_records(sql)

            if not records or not records[0]:
                raise ValueError(f"Test {i} returned no results. SQL: {sql}")

            actual = records[0][0]

            if actual != expected:
                raise ValueError(
                    f"Data quality test {i} failed. Expected {expected}, got {actual}. SQL: {sql}"
                )

            self.log.info("Test %s passed (expected=%s, got=%s)", i, expected, actual)

        self.log.info("DataQualityOperator completed - all tests passed")
