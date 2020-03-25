from os import environ as env
import pyspark
from typing import List, Tuple
from decimal import Decimal
from itertools import chain
from more_itertools import chunked

import worker
from storage import StorageClient
from google.cloud.storage.blob import Blob


def main():
    # GCSからデータを取得
    storage_client: StorageClient = StorageClient(env['PROJECT_ID'], env['BUCKET_NAME'])
    sentence_blob: Blob = storage_client.download_from_gcs('dataproc/data.txt')[0]  # 1件目取得
    sentences: List[str] = sentence_blob.download_as_string().decode().split('\n')  # 改行で区切った文書リスト

    # 5件単位にリストを分割
    chuncked_sentences: List[List[str]] = list(chunked(sentences, 5))

    # 分散処理
    sc = pyspark.SparkContext()
    rdd = sc.parallelize(chuncked_sentences)
    vectors: List[List[Tuple[int, Decimal]]] = list(chain.from_iterable(
        rdd.map(lambda sentences: worker.vectorize(sentences)).collect()
    ))

    print(vectors)


if __name__ == "__main__":
    main()
