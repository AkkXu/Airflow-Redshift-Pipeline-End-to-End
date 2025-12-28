from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql_query="",
        truncate_table=True,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info("Truncating dimension table %s", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table};")

        self.log.info("Loading dimension table %s", self.table)
        redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
