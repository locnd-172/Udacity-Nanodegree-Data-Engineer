from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            
            query = test.get("query")
            expected = test.get("expected")

            records = redshift.get_records(query)
          
            if records[0][0] == expected:
                message = f"Data quality check PASSED with {records[0][0]} records"
            else:
                message = f"Data quality check FAILED with {records[0][0]} records"
                # raise ValueError(message)
            
            self.log.info(message)
        
        self.log.info("Data quality check completed!!!")