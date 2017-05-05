# csci63-SparkstreamingIOT
csci63-SparkstreamingIOT

kafka_flightdatastream_producer : Python Kafka producer reading flight stream csv and sending 500 records at a time to kafka topic

spark_cassandra_flightdata : Pyspark consumer reading data from kafka topic as dstream (2 - window interval ,2 - sliding interval); 
  load data as-is to cassandra table flightiot for further analytics; And filter flights with delays - join with customers dataset - load flightdelays cassandra table for notification events;   
