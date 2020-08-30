from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON data from S3 to staging tables in Redshift data warehouse

    _Please Note_
        two global Variables are in used:
        ACCESS_KEY_ID & SECRET_ACCESS_KEY
        I do not use `global` - it's for my opinion a better practice
        to init all needed Variables directly in a function or class,
        not outside.
        The CAP_CASE should suggest you, that this shoul be a global var.

    Parameters:
    * `redshift_conn_id`: Redshift Connection ID (string)
    * `aws_credentials_id`: AWS Credentials ID (string)
    * `s3_path`: Path to s3 where file is stored (string)
    * `file_type`: Given Filetype of data (JSON / csv / ... ) (string)
    * `copy_option`: mapping the elements in source data to the columns into
                     target table (string)
    * `table`: Target staging table in Redshift to copy data into (string)
    """

    ui_color = '#358140'

    delete_sql = "DELETE FROM {};"

    copy_sql = """
        COPY {}
        FROM '{}'
        FORMAT as {} '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_path="",
                 file_type='json',
                 copy_option='auto',
                 table='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.file_type = file_type
        self.copy_option = copy_option
        self.table = table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(self.delete_sql.format(self.table))

        self.log.info("Copy data from S3 to Redshift")
        formatted_sql = self.copy_sql.format(
            self.table,
            self.s3_path,
            self.file_type,
            self.copy_option,
            credentials.access_key,
            credentials.secret_key
        )
        self.log.info('Run Copy')
        redshift.run(formatted_sql)
