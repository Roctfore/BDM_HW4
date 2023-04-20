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

def extractFeatures2(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (store, department, code, name, price) = (row[0], row[1], row[2][4:], row[3], row[5].split('$')[1].split('聽')[0])
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0each', ''))
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0per lb', ''))
        yield (store, department, code, name, price)

def main():
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    KSI = 'keyfood_sample_items.csv'
    KP = 'keyfood_products.csv'

    sample_items = sc.textFile(KSI, use_unicode=True).cache()
    products = sc.textFile(KP, use_unicode=True).cache()

    sample_items = sample_items.mapPartitionsWithIndex(extractFeatures1)
    products = products.mapPartitionsWithIndex(extractFeatures2)

    with open('keyfood_nyc_stores.json', 'r') as f:
        stores = json.load(f)

    store = {store_id: (store['communityDistrict'], store['foodInsecurity']) 
                for store_id, store in stores.items()}

    rdd = sc.parallelize(list(store.items()))
    security = rdd.map(lambda x: (x[0], x[1][0], round(x[1][1],2)))
    insecurity_dict = security.map(lambda x: (x[0], x[2])).collectAsMap()

    result_rdd = products.map(lambda x: (x[0], x[2], x[3], x[4], insecurity_dict.get(x[0])))

    sample_names = [x[1] for x in sample_items.collect()]
    filtered_rdd = result_rdd.filter(lambda x: x[1] in sample_names)

    sample_dict = dict(sample_items.collect())
    filtered_rdd = filtered_rdd.filter(lambda x: x[1] in sample_dict)

    outputTask1 = filtered_rdd.map(lambda x: (sample_dict[x[1]], x[3], x[4]))
    outputTask1.cache()

    print(outputTask1.take(5))
    print(outputTask1.count())

if __name__ == "__main__":
    main()
