#!/usr/bin/env python
import sys
import os
import json
import pyspark

# add actual job
def doJob(rdd):
  return rdd.map(lambda a: (a.split(',')[1], a.split(',')[2], a.split(',')[5], a.split(',')[6], a.split(',')[9], a.split(',')[10]))\
    .map(lambda b: (b[0].split(' ')[1].split(':')[0], b[1].split(' ')[1].split(':')[0], b[2], b[3], b[4], b[5]))\
    .filter(lambda c: int(c[0])>6 and int(c[0])<10 or int(c[0])>14 and int(c[0])<19)\
    .filter(lambda d: d[2] == '40.751873' and d[3] == '-73.977706')\
    .map(lambda e: ((e[4], e[5]), 1))\
    .reduceByKey(lambda f, g: f+g)\
    .sortBy(lambda h: -h[1])

def main():
  # parse arguments 
  in_dir, out_dir = sys.argv[1:]
  
  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)
  
  # invoke job and put into output directory
  doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
