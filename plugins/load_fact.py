from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql_query="",
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info("Loading fact table %s", self.table)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_sql)

        self.log.info("LoadFactOperator completed for %s", self.table)
