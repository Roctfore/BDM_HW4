#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
import csv
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import SparkContext

KSI = 'keyfood_sample_items.csv'
KP = '/shared/CUSP-GX-6002/data/keyfood_products.csv'
ST = 'keyfood_nyc_stores.json'

def extractFeatures1(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (code, name) = (row[0][4:], row[1])
        yield (code, name)

def extractFeatures2(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (store, department, code, name, price) = (row[0], row[1], row[2][4:], row[3], row[5].split('$')[1].split('聽')[0])
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0each', ''))
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0per lb', ''))
        yield (store, department, code, name, price)

if __name__ == "__main__":
    output_dir = sys.argv[1]

    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.session.timeZone", "EST")

    sample_items = sc.textFile(KSI, use_unicode=True).cache()
    products = sc.textFile(KP, use_unicode=True).cache()
    sample_items = sample_items.mapPartitionsWithIndex(extract_features_ksi)
    products = products.mapPartitionsWithIndex(extract_features_kp)
    
    prod = sc.textFile(KP, use_unicode=True)             .filter(lambda x: not x.startswith('store,department'))             .map(lambda r:  next(csv.reader([r])) )             .filter(lambda x: x[2]!='N/A')             .map(lambda x: (x[0],(x[2].split('-')[1]),x[3], x[5][1:].split('\xa0')[0]) )

    with open(ST, 'r') as f:
        stores = json.load(f)
    store = {store_id: (store['communityDistrict'], store['foodInsecurity']) 
                for store_id, store in stores.items()}
    rdd = sc.parallelize(list(store.items()))
    security = rdd.map(lambda x: (x[0], x[1][0], round(x[1][1],2)))
    insecurity_dict = security.map(lambda x: (x[0], x[2])).collectAsMap()
    result_rdd = prod.map(lambda x: (x[0], x[1], x[2],x[3], insecurity_dict.get(x[0])))
    rdd2 = result_rdd.map(lambda x: (x[2], x[3], x[4]*100))
    sample_names = [x[1] for x in sample_items.collect()]
    filtered_rdd2 = rdd2.filter(lambda x: x[0] in sample_names)
    sample_dict = dict(sample_items.collect())
    filtered_rdd2 = result_rdd.filter(lambda x: x[1] in sample_dict)
    outputTask1 = filtered_rdd2.map(lambda x: (sample_dict[x[1]], x[3], x[4]))
    outputTask1 = outputTask1.cache()

    outputTask1         .coalesce(1)         .map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}")         .saveAsTextFile(output_dir)

    sc.stop()

