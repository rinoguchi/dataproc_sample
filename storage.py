import os
from typing import Optional, List

from google.oauth2.service_account import Credentials
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob


class StorageClient:
    project_id: str
    bucket_name: str
    storage_client: storage.Client
    bucket: Bucket

    def __init__(self, project_id: str, bucket_name: str, storage_credential_path: Optional[str] = None):
        self.project_id = project_id
        self.bucket_name = bucket_name
        if storage_credential_path is not None:
            storage_credentials = Credentials.from_service_account_file(storage_credential_path)
            self.storage_client = storage.Client(project=project_id, credentials=storage_credentials)
        else:
            self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    def upload_to_gcs(self, file_path: str, gcs_base_path: str) -> str:
        blob: Blob = self.bucket.blob(f'{gcs_base_path}/{os.path.basename(file_path)}')
        blob.upload_from_filename(file_path)
        return f'gs://{self.bucket_name}/{blob.name}'

    def download_from_gcs(self, prefix: str) -> List[Blob]:
        return list(self.bucket.list_blobs(prefix=prefix))
