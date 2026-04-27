import boto3
from botocore.exceptions import ClientError
from dagster import ConfigurableResource, EnvVar


class MinIOResource(ConfigurableResource):
    endpoint_url: str = EnvVar("MINIO_ENDPOINT")
    access_key: str = EnvVar("MINIO_ACCESS_KEY")
    secret_key: str = EnvVar("MINIO_SECRET_KEY")
    bucket: str = EnvVar("MINIO_MAIL_BUCKET")

    def _client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def download_bytes(self, key: str) -> bytes:
        response = self._client().get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def list_objects(self, prefix: str = "") -> list[dict]:
        client = self._client()
        paginator = client.get_paginator("list_objects_v2")
        results = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            results.extend(page.get("Contents", []))
        return results

    def object_exists(self, key: str) -> bool:
        try:
            self._client().head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
