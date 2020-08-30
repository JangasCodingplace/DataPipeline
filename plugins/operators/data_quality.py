from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by counting rows in tables

    Parameters:
    * `redshift_conn_id`: Redshift Connection ID (string)
    * `tables`: Tables to check (list of strings)
    """

    ui_color = '#89DA59'

    select_sql = "SELECT COUNT(*) FROM {};"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(self.select_sql.format(table))

            if records is None or len(records[0]) < 1:
                self.log.error(
                    f"No Records Presents in destination table {table}"
                )
                raise ValueError(
                    f"No Records Presents in destination table {table}"
                )

            self.log.info(
                f"""
                Data Quality Check on table {table} check
                passed with {records[0][0]} records
                """
            )
