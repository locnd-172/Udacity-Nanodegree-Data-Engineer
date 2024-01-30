from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_query="",
                 insert_query="",
                 target_table="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_query = create_query
        self.insert_query = insert_query
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating fact table")
        redshift.run(format(self.create_query))

        if self.truncate:
            self.log.info(f"Execute - Truncate fact table {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f'Loading fact table {self.target_table}')
        redshift.run(f"INSERT INTO {self.target_table} {self.insert_query}")
        
        self.log.info("Load Fact table completed!!!")