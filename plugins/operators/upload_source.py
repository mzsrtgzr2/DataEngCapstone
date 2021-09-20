import requests
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class UploadSourceOperator(BaseOperator):

    ui_color = '#dd42f5'

    @apply_defaults
    def __init__(self,
                aws_conn_id: str,
                source_url: str,
                bucket_name: str,
                key: str,
                 *args, **kwargs):

        super(UploadSourceOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.source_url = source_url
        self.bucket_name = bucket_name
        self.key = key

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        
        req = requests.get(self.source_url)

        # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html#airflow.providers.amazon.aws.hooks.s3.S3Hook.load_bytes
        s3.load_bytes(
            bytes_data=req.content,
            bucket_name=self.bucket_name,
            replace=True,
            key=self.key
        )
