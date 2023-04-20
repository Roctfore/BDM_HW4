#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
import sys
import csv
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# set up SparkSession
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

# define input file paths
KSI = 'keyfood_sample_items.csv'
KP = '/shared/CUSP-GX-6002/data/keyfood_products.csv'
KNS = 'keyfood_nyc_stores.json'

# define function to extract features from keyfood_sample_items.csv
def extract_features_ksi(partId, records):
    if partId == 0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (code, name) = (row[0][4:], row[1])
        yield (code, name)

# define function to extract features from keyfood_products.csv
def extract_features_kp(partId, records):
    if partId == 0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (store, department, code, name, price) = (row[0], row[1], row[2][4:], row[3], row[5].split('$')[1].split('聽')[0])
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0each', ''))
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0per lb', ''))
        yield (store, department, code, name, price)

# extract features from keyfood_sample_items.csv and keyfood_products.csv
sample_items = sc.textFile(KSI, use_unicode=True).cache()
products = sc.textFile(KP, use_unicode=True).cache()
sample_items = sample_items.mapPartitionsWithIndex(extract_features_ksi)
products = products.mapPartitionsWithIndex(extract_features_kp)

# extract features from keyfood_nyc_stores.json and create insecurity_dict
with open(KNS, 'r') as f:
    stores = json.load(f)
store = {store_id: (store['communityDistrict'], store['foodInsecurity']) 
         for store_id, store in stores.items()}
rdd = sc.parallelize(list(store.items()))
security = rdd.map(lambda x: (x[0], x[1][0], round(x[1][1],2)))
insecurity_dict = security.map(lambda x: (x[0], x[2])).collectAsMap()

# join products and insecurity_dict on store ID
result_rdd = products.map(lambda x: (x[0], x[1], x[2], x[3], insecurity_dict.get(x[0])))

# filter result_rdd by sample_items
sample_dict = dict(sample_items.collect())
filtered_rdd = result_rdd.filter(lambda x: x[3] in sample_dict)

# format output and save to output folder
output_folder = sys.argv[1]
output_rdd = filtered_rdd.map(lambda x: (sample_dict[x[3]], x[2], x[4]))
output_rdd.saveAsTextFile(output_folder)

