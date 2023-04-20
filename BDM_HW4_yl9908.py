# BDM_HW4_yl9908.py

import csv
import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession

def extractFeatures1(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (code, name) = (row[0][4:], row[1])
        yield (code, name)

def main(output_folder):
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    KSI = 'keyfood_sample_items.csv'
    KP = '/shared/CUSP-GX-6002/data/keyfood_products.csv'

    sample_items = sc.textFile(KSI, use_unicode=True).cache()
    products = sc.textFile(KP, use_unicode=True).cache()

    sample_items = sample_items.mapPartitionsWithIndex(extractFeatures1)

    prod = products.filter(lambda x: not x.startswith('store,department')) \
        .map(lambda r:  next(csv.reader([r])) ) \
        .filter(lambda x: x[2]!='N/A') \
        .map(lambda x: (x[0],(x[2].split('-')[1]),x[3], x[5][1:].split('\xa0')[0]) )

    with open('keyfood_nyc_stores.json', 'r') as f:
        stores = json.load(f)

    store = {store_id: (store['communityDistrict'], store['foodInsecurity']) 
                for store_id, store in stores.items()}

    rdd = sc.parallelize(list(store.items()))
    security = rdd.map(lambda x: (x[0], x[1][0], round(x[1][1],2)))
    insecurity_dict = security.map(lambda x: (x[0], x[2])).collectAsMap()

    result_rdd = prod.map(lambda x: (x[0], x[1], x[2], x[3], insecurity_dict.get(x[0])))

    sample_names = [x[1] for x in sample_items.collect()]
    filtered_rdd = result_rdd.filter(lambda x: x[1] in sample_names)

    sample_dict = dict(sample_items.collect())
    filtered_rdd = filtered_rdd.filter(lambda x: x[1] in sample_dict)

    outputTask1 = filtered_rdd.map(lambda x: (sample_dict[x[1]], x[3], x[4]))
    outputTask1.cache()

    outputTask1.saveAsTextFile(output_folder)

if __name__ == "__main__":
    output_folder = sys.argv[1]
    main(output_folder)
