from typing import List
import os
from os import environ as env
from dataproc import DataprocCluster
from storage import StorageClient

SENTENCES: List[str] = [
    "Good words cool more than c",
    "Great talkers are like leaky pitchers, everything runs out of them.",
    "Every path has a puddle.",
    "Fools grow without watering.",
    "We never know the worth of water till the well is dry.",
    "Don't go near the water until you learn how to swim.",
    "Fire and water may be good servants, but bad masters.",
    "Let the past drift away with the water.",
    "Fish must swim thrice; once in the water, a second time in the sauce, and a third time in wine in the stomach.",
    "A sieve will hold water better than a woman's mouth a secret.",
    "If the lad go to the well against his will, either the can will break or the water will spill.",
    "Don't throw out your dirty water until you get in fresh.",
    "A straight stick is crooked in the water.",
    "You never miss the water till the well runs dry.",
    "Drinking water neither makes a man sick, nor in debt, nor his wife a widow.",
    "Mills will not grind if you give them not water.",
    "The pitcher goes so often to the well that it is broken at last.",
    "The mill cannot grind with the water that is past.",
    "The mill gets by going.",
    "A monk out of his cloister is like a fish out of water.",
    "Standing pools gather filth.",
]


def main():
    # 実行するpythonファイルをGCSにアップロード（事前に手作業でアップロードしてもOK）
    storage_client: StorageClient = StorageClient(env['BUCKET_NAME'], env['PROJECT_ID'], env['STORAGE_CREDENTIAL_PATH'])
    main_python_file_uri: str = storage_client.upload_to_gcs('./master.py', 'dataproc')
    python_file_uris: List[str] = [storage_client.upload_to_gcs(path, 'dataproc') for path in ['./worker.py', './storage.py']]

    # 処理対象データをGCSにアップロード（事前に手作業でアップロードしてもOK）
    data_file_path: str = './data.txt'
    with open(data_file_path, 'w') as f:
        for sentence in SENTENCES:
            f.write(sentence + '\n')
    storage_client.upload_to_gcs(data_file_path, 'dataproc')
    os.remove(data_file_path)

    # pysparkのjobを実行
    with DataprocCluster(
        env['PROJECT_ID'],
        env['DATAPROC_CREDENTIAL_PATH'],
        cluster_name='test-cluster',
        creates_cluster=True,
        pip_packages='more-itertools==5.0.0 nltk==3.4.5 gensim==3.8.1 google-cloud-storage==1.20.0',
        environment_variables={'PROJECT_ID': env['PROJECT_ID'], 'BUCKET_NAME': env['BUCKET_NAME']}
    ) as cluster:
        cluster.submit_pyspark_job(main_python_file_uri, python_file_uris)
        print('do something')


if __name__ == "__main__":
    main()
