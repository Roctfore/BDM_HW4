#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python

import csv
import json
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Define paths to the input files
KSI = 'keyfood_sample_items.csv'
KP = '/shared/CUSP-GX-6002/data/keyfood_products.csv'
KNS = 'keyfood_nyc_stores.json'

# Set up Spark
conf = SparkConf().setAppName("BDM_HW4")
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# Define a function to extract the features from the sample items CSV file
def extractFeatures1(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (code, name) = (row[0][4:], row[1])
        yield (code, name)

# Define a function to extract the features from the products CSV file
def extractFeatures2(partId, records):
    if partId==0: 
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (store, department, code, name, price) = (row[0], row[1], row[2][4:], row[3], row[5].split('$')[1].split('聽')[0])
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0each', ''))
        (store, department, code, name, price) = (store, department, code, name, price.replace('\xa0per lb', ''))
        yield (store, department, code, name, price)
        
def main(OUTPUT_FOLDER_NAME):
    # Load the sample items and products CSV files
    sample_items = sc.textFile(KSI, use_unicode=True).cache()
    products = sc.textFile(KP, use_unicode=True).cache()

    # Extract the features from the sample items and products
    sample_items = sample_items.mapPartitionsWithIndex(extractFeatures1)
    products = products.mapPartitionsWithIndex(extractFeatures2)

    # Load the Key Food NYC stores JSON file
    with open(KNS, 'r') as f:
        stores = json.load(f)

    # Map the store IDs to their community district and food insecurity level
    store = {store_id: (store['communityDistrict'], store['foodInsecurity']) 
                for store_id, store in stores.items()}

    # Create an RDD from the store dictionary
    rdd = sc.parallelize(list(store.items()))

    # Map the store IDs to their food insecurity level
    security = rdd.map(lambda x: (x[0], x[1][0], round(x[1][1],2)))
    insecurity_dict = security.map(lambda x: (x[0], x[2])).collectAsMap()

    # Map the product data to include the food insecurity level of the store where it is sold
    result_rdd = products.map(lambda x: (x[0], x[1], x[2], x[3], insecurity_dict.get(x[0])))

    # Map the product data to include the sample item name
    sample_names = [x[1] for x in sample_items.collect()]
    filtered_rdd2 = result_rdd.filter(lambda x: x[2] in sample_names)

    # Create a dictionary of the sample items
    sample_dict = dict(sample_items.collect())

    # Filter the product data to only include sample items
    filtered_rdd2 = result_rdd.filter(lambda x: x[0] in sample_names)

    sample_dict = dict(sample_items.collect())
    filtered_rdd2 = result_rdd.filter(lambda x: x[1] in sample_dict)
    outputTask1 = filtered_rdd2.map(lambda x: (sample_dict[x[1]], x[3], x[4]))
    outputTask1.cache()

    outputTask1.saveAsTextFile(output_folder)
    
if __name__ == "__main__":
    OUTPUT_FOLDER_NAME = sys.argv[1]
    main(OUTPUT_FOLDER_NAME)

