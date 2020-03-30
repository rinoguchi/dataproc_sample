import traceback
import time
import random
import string
from typing import List, Dict

from google.cloud.dataproc_v1 import ClusterControllerClient, JobControllerClient
from google.cloud.dataproc_v1.gapic.transports.cluster_controller_grpc_transport import ClusterControllerGrpcTransport
from google.cloud.dataproc_v1.gapic.transports.job_controller_grpc_transport import JobControllerGrpcTransport
from google.oauth2.service_account import Credentials
from google.api_core.operation import Operation
from google.protobuf.duration_pb2 import Duration


class DataprocCluster:
    project_id: str
    region: str
    zone: str
    cluster_client: ClusterControllerClient
    cluster_name: str
    dataproc_credentials: Credentials
    creates_cluster: bool
    waiting_callback: bool

    def __init__(
        self,
        project_id: str,
        dataproc_credential_path: str,
        region: str = 'asia-east1',
        zone: str = 'asia-east1-a',
        cluster_name='cluster-' + ''.join(random.choices(string.ascii_lowercase, k=10)),
        creates_cluster: bool = True,
        master_machine_type: str = 'n1-standard-1',
        num_master_instances: int = 1,
        worker_machine_type: str = 'n1-standard-1',
        num_worker_instances: int = 2,
        idle_delete_ttl: Duration = Duration(seconds=3600),  # defaultは1時間
        pip_packages: str = '',
        environment_variables: Dict[str, str] = dict()
    ):
        self.project_id = project_id
        self.region = region
        self.zone = zone
        self.dataproc_credentials = Credentials.from_service_account_file(dataproc_credential_path)
        client_transport: ClusterControllerGrpcTransport = ClusterControllerGrpcTransport(
            address='{}-dataproc.googleapis.com:443'.format(self.region),
            credentials=self.dataproc_credentials
        )
        self.cluster_client = ClusterControllerClient(client_transport)
        self.cluster_name = cluster_name
        self.creates_cluster = creates_cluster
        if not self.creates_cluster:
            return

        print(f'create_cluster {self.cluster_name} started.')

        properties: Dict[str, str] = dict()
        properties['yarn:yarn.nodemanager.vmem-check-enabled'] = 'false'  # yarn-site.xmlの形式
        for item in environment_variables.items():
            properties[f'spark-env:{item[0]}'] = item[1]  # spark-env.shの形式

        cluster_data = {
            'project_id': self.project_id,
            'cluster_name': self.cluster_name,
            'config': {
                'software_config': {
                    'image_version': '1.4-ubuntu18',
                    'properties': properties
                },
                'lifecycle_config': {
                    'idle_delete_ttl': idle_delete_ttl
                },
                'initialization_actions': [{
                    'executable_file': 'gs://dataproc-initialization-actions/python/pip-install.sh'
                }],
                'gce_cluster_config': {
                    'zone_uri': f'https://www.googleapis.com/compute/v1/projects/{self.project_id}/zones/{self.zone}',
                    'metadata': {
                        'PIP_PACKAGES': pip_packages
                    }
                },
                'master_config': {
                    'num_instances': num_master_instances,
                    'machine_type_uri': master_machine_type,
                    'disk_config': {
                        'boot_disk_size_gb': 128
                    }
                },
                'worker_config': {
                    'num_instances': num_worker_instances,
                    'machine_type_uri': worker_machine_type,
                    'disk_config': {
                        'boot_disk_size_gb': 128
                    }
                }
            }
        }

        response: Operation = self.cluster_client.create_cluster(self.project_id, self.region, cluster_data)
        response.add_done_callback(self.__callback)
        self.waiting_callback = True
        self.__wait_for_callback()
        print(f'create_cluster {self.cluster_name} finished.')

    def __callback(self, operation_future):
        print('callback called.')
        print(operation_future.result())
        self.waiting_callback = False

    def __wait_for_callback(self):
        print('waiting for callback call...')
        while True:
            if not self.waiting_callback:
                break
            time.sleep(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if tb is not None:
            print(''.join(traceback.format_tb(tb)))
        if not self.creates_cluster:
            return

        print(f'delete_cluster {self.cluster_name} started.')
        response: Operation = self.cluster_client.delete_cluster(self.project_id, self.region, self.cluster_name)
        response.add_done_callback(self.__callback)
        self.waiting_callback = True
        self.__wait_for_callback()
        print(f'delete_cluster {self.cluster_name} finished.')

    def submit_pyspark_job(self, main_python_file_uri: str, python_file_uris: List[str]):
        print(f'submit pyspark job started.')
        job_details = {
            'placement': {
                'cluster_name': self.cluster_name
            },
            'pyspark_job': {
                'main_python_file_uri': main_python_file_uri,
                'python_file_uris': python_file_uris
            }
        }

        job_transport: JobControllerGrpcTransport = JobControllerGrpcTransport(
            address='{}-dataproc.googleapis.com:443'.format(self.region),
            credentials=self.dataproc_credentials
        )
        dataproc_job_client = JobControllerClient(job_transport)

        result = dataproc_job_client.submit_job(
            project_id=self.project_id, region=self.region, job=job_details)
        job_id = result.reference.job_id
        print(f'job {job_id} is submitted.')

        print(f'waiting for job {job_id} to finish...')
        while True:
            time.sleep(1)
            job = dataproc_job_client.get_job(self.project_id, self.region, job_id)
            if job.status.State.Name(job.status.state) == 'ERROR':
                raise Exception(job.status.details)
            elif job.status.State.Name(job.status.state) == 'DONE':
                print(f'job {job_id} is finished.')
                break
