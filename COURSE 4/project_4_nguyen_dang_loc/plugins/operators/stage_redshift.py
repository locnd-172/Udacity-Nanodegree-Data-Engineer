from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    copy_sql_stmt = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_query="",
                 target_table="",
                 s3_path="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_query = create_query
        self.target_table = target_table
        self.s3_path = s3_path
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating staging table")
        redshift.run(format(self.create_query))

        credentials = aws_hook.get_credentials()
        
        self.log.info("Clearing data in staging table")
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Copy data from S3 to Redshift")
        
        formatted_sql = StageToRedshiftOperator.copy_sql_stmt.format(
            self.target_table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)
        self.log.info("Stage to Redshift completed!!!")