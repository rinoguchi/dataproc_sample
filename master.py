from os import environ as env
import pyspark
from typing import List, Tuple
from decimal import Decimal
from itertools import chain
from more_itertools import chunked

from google.cloud.storage.blob import Blob

import worker
try:
    from module.storage import StorageClient
except ModuleNotFoundError:  # pysparkノード上、全てのソースはGCS上の見かけ上のフォルダ構成は無視されて、同一フォルダ配下に配備されるため
    from storage import StorageClient


def main():
    # GCSからデータを取得
    storage_client: StorageClient = StorageClient(env['BUCKET_NAME'])
    sentence_blob: Blob = storage_client.download_from_gcs('dataproc/input/data.txt')[0]  # 1件目取得
    sentences: List[str] = sentence_blob.download_as_string().decode().split('\n')  # 改行で区切った文書リスト

    # 5件単位にリストを分割
    chuncked_sentences: List[List[str]] = list(chunked(sentences, 5))
    chuncked_sentences_indexes: List[int] = range(len(chuncked_sentences))

    # 分散処理
    sc = pyspark.SparkContext()
    rdd = sc.parallelize(chuncked_sentences_indexes)
    broadcasted_chuncked_sentences = sc.broadcast(chuncked_sentences)
    vectors: List[List[Tuple[int, Decimal]]] = list(chain.from_iterable(
        rdd.map(lambda index: worker.vectorize(broadcasted_chuncked_sentences.value[index])).collect()
    ))

    print(vectors)


if __name__ == "__main__":
    main()
