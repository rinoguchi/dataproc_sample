from pyspark import SparkContext, RDD
from typing import List, Dict
import time

# inputデータ（試験の点数）
input_data: List[Dict[str, int]] = [
    {'国語': 86, '算数': 57, '理科': 45, '社会': 100},
    {'国語': 67, '算数': 12, '理科': 43, '社会': 54},
    {'国語': 98, '算数': 98, '理科': 78, '社会': 69},
]
print(f'input: {input_data}')

# RDD作成
sc: SparkContext = SparkContext(appName='spark_sample')
sc.setLogLevel('INFO')

rdd: RDD = sc.parallelize(input_data)

# 各教科および合計点の平均点を計算
output: Dict[str, float] = rdd\
    .map(lambda x: x.update(合計=sum(x.values())) or x)\
    .flatMap(lambda x: x.items())\
    .groupByKey()\
    .map(lambda x: (x[0], round(sum(x[1]) / len(x[1]), 2)))\
    .collect()

print(f'output: {output}')
# time.sleep(100)
