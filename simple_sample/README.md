# simpleサンプル
同一の分散処理実装（main.py）を３つの方法で実行してみる

## 実行方法
### pysparkパッケージ
```sh
# パッケージインストール
pip install pyspark
pip install py4j
# 実行
python main.py
```

### Spark Standalone Modeで実行
```sh
# apache-sparkをインストール
brew install apache-spark
export SPARK_HOME=/usr/local/Cellar/apache-spark/2.4.5/libexec

# Master Node立ち上げ
${SPARK_HOME}/sbin/start-master.sh
# http://localhost:8080にアクセスして、Master-URLを確認 => spark://hoge-machine:7077

# Worker Node立ち上げ
${SPARK_HOME}/sbin/start-slave.sh -c 2 spark://hoge-machine:7077

# ジョブをsubmit
spark-submit main.py --master spark://hoge-machine:7077
```

### Google Cloud Dataproc（CLI）で実行
```sh
# Dataproc環境準備
# * Dataproc API有効化
# * GCS Bucket作成
# * サービスアカウント作成（dataproc編集者, stogage管理者ロール）

# サービスアカウントを有効化
gcloud auth activate-service-account --key-file {key_file_json_path} --project {project_name}

# ソースコードをGCSにupload
gsutil cp main.py gs://{bucket_name}/dataproc/src/

# クラスタを立ち上げ
gcloud dataproc clusters create spark-sample-cluster\
  --bucket={bucket_name} --region=asia-east1 --image-version=1.4\  
  --master-machine-type=n1-standard-1 --worker-machine-type=n1-standard-1\
  --num-workers=2 --worker-boot-disk-size=256 --max-idle=1h

# ジョブをsubmit
cloud dataproc jobs submit pyspark\
  --cluster=spark-sample-cluster --region=asia-east1\
  gs://rinoguchi_dataproc-test/dataproc/src/simple/main.py
```