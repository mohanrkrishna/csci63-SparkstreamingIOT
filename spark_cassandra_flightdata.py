from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys

def loadcassandra(time, rdd,ctable):
   """
    Invoked by foreach against dtream to loop through all rdds;
   """
   if (not rdd.isEmpty()):  #Check if rdd not empty to avoid empty files
       timesuffix = int(time.strftime('%s'))
       rdd.write.format("org.apache.spark.sql.cassandra"). \
           options(table=ctable, keyspace="flightkeyspace"). \
           save(mode="append")

       print(str(time.strftime('%x %X')) + '...loading cassandra flightdata')


def processdstream(rdd):
    flightdelays_collist = [origin,flightnum,flightdate,email,origincityname,dest,destcityname,airtime,arrdelay,arrdelayminutes,arrivaldelaygroups,
         arrtime,arrtimeblk,cancellationcode,cancelled,carrierdelay,departuredelaygroups,depdelay,depdelayminutes,deptime,deptimeblk,destairportid,deststate,
         distance,distancegroup,diverted,first_name,flights,id,last_name,lateaircraftdelay,nasdelay,originairportid,originstate,phone,securitydelay,weatherdelay]

    fstatusstream=sql.read.json(rdd)
    fstatusstream_delay=fstatusstream.filter((fstatusstream.depdelay !=0) | (fstatusstream.arrdelay !=0))
    customers_delay = fstatusstream_delay.join(customer,[flightnum,origin,dest],'inner'). \
                      select(flightdelays_collist)
    return customers_delay

conf=SparkConf().setAppName("FlightIOTStreaming")
sc = SparkContext(appName="FlightIOTStreaming")
sql = SQLContext(sc)
ssc = StreamingContext(sc, 2)

brokers, topic = sys.argv[1:]

flightstream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#flightstream.pprint()
flightdata = flightstream.map(lambda kv:kv[1])


flightwindow = flightdata.window(2,2)

customer = sql.read.format("org.apache.spark.sql.cassandra"). \
           load(keyspace="flightkeyspace", table="customer").cache()

customers_delay = flightwindow.transform(processdstream)

flightwindow.foreachRDD(loadcassandra,"flightiot")
customers_delay.foreachRDD(loadcassandra,"flighdelays")

sc.setCheckpointDir("hdfs:///user/spark/projects/iotcheckpoint/")
ssc.start()

ssc.awaitTermination()