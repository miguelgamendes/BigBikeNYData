#!/usr/bin/env python
import sys
import os
import json
import pyspark

# add actual job
def doJob(rdd):
  return rdd.map(lambda a: (a.split(',')[1], a.split(',')[2], a.split(',')[5], a.split(',')[6]))\
    .map(lambda b: (b[0].split(' ')[1].split(':')[0], b[1].split(' ')[1].split(':')[0], b[2], b[3]))\
    .filter(lambda c: int(c[0])<7 or int(c[0])>9 and int(c[0])<15 or int(c[0])>18)\
    .map(lambda d: ((d[2], d[3]), 1))\
    .reduceByKey(lambda e, f: e+f)\
    .sortBy(lambda g: -g[1])

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  
  # invoke job and put into output directory
  doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
