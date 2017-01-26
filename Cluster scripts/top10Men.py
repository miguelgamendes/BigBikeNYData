#!/usr/bin/env python
import sys
import os
import json
import pyspark

# add actual job
def doJob(rdd):
  return rdd.map(lambda a: (a.split(',')[5], a.split(',')[6], a.split(',')[-1]))\
    .filter(lambda b: b[2] == '1')\
    .map(lambda c: ((c[0], c[1]), 1))\
    .reduceByKey(lambda d, e: d+e)\
    .sortBy(lambda f: -f[1])

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  
  # invoke job and put into output directory
  doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
