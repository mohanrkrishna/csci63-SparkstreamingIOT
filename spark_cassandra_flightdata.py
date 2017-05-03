from __future__ import print_function

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

def savefunc(time, rdd):
   """
    Invoked by foreach against dtream to loop through all rdds; Save to HDFS if not empty after bring data to single partition to avoid multiple files;
   """
   if (not rdd.isEmpty()):  #Check if rdd not empty to avoid empty files
       timesuffix = int(time.strftime('%s'))
       filepath = "hdfs:///user/spark/projects/flightiot/output" + "-" + str(timesuffix)
       df1=sql.read.json(rdd)
       df2=df1.filter((df1.DepDelay !=0) | (df1.ArrDelay !=0))
       df2.write.format('json').save(filepath)
       print(str(time.strftime('%x %X')) + '...saving hdfs')

conf=SparkConf().setAppName("FlightIOTStreaming")
sc = SparkContext(appName="FlightIOTStreaming")
sql = SQLContext(sc)
ssc = StreamingContext(sc, 2)

brokers, topic = sys.argv[1:]

flightstream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#flightstream.pprint()
flightdata = flightstream.map(lambda kv:kv[1])
#flightdata = flightstream.map(lambda kv:json.loads(kv[1]))
#                         .filter(lambda js: js['DepDelay']!=0 or js['ArrDelay']!=0 )

flightwindow = flightdata.window(2,2)

#tbl_flightiot = sql.read.format("org.apache.spark.sql.cassandra").\
#               load(keyspace="flightiot", table="flightdata")
flightwindow.foreachRDD(savefunc)

sc.setCheckpointDir("hdfs:///user/spark/projects/iotcheckpoint/")
ssc.start()

ssc.awaitTermination()