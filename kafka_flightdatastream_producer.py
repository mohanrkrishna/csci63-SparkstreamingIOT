from kafka import KafkaProducer
import pickle
import time
from datetime import datetime
from itertools import islice
import sys
import zipfile
import pandas as pd

producer = KafkaProducer(bootstrap_servers='ec2-52-91-250-49.compute-1.amazonaws.com:9092')
topic = 'airline1'

zfile = zipfile.ZipFile("On_Time_On_Time_Performance_2017_1.zip")
filename = zfile.filelist[0].filename
print(filename)
filecontent = zfile.extract(filename)
columns_list=['Year', 'Quarter', 'Month' ,'DayofMonth' ,'DayOfWeek', 'FlightDate',
 'UniqueCarrier' ,'AirlineID', 'Carrier' ,'TailNum' ,'FlightNum',
 'OriginAirportID', 'OriginAirportSeqID', 'OriginCityMarketID', 'Origin',
 'OriginCityName', 'OriginState', 'OriginStateFips','OriginStateName',
 'OriginWac', 'DestAirportID', 'DestAirportSeqID', 'DestCityMarketID' ,'Dest',
 'DestCityName', 'DestState', 'DestStateFips' ,'DestStateName', 'DestWac',
 'CRSDepTime', 'DepTime' ,'DepDelay' ,'DepDelayMinutes' ,'DepDel15',
 'DepartureDelayGroups' ,'DepTimeBlk', 'TaxiOut', 'WheelsOff', 'WheelsOn',
 'TaxiIn' ,'CRSArrTime' ,'ArrTime', 'ArrDelay', 'ArrDelayMinutes' ,'ArrDel15',
 'ArrivalDelayGroups' ,'ArrTimeBlk' ,'Cancelled', 'CancellationCode',
 'Diverted' ,'CRSElapsedTime', 'ActualElapsedTime', 'AirTime','Flights',
 'Distance' ,'DistanceGroup', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
 'SecurityDelay', 'LateAircraftDelay', 'FirstDepTime' ,'TotalAddGTime',
 'LongestAddGTime']

df_itr=pd.read_csv(filecontent,dtype={'cancellationcode': str, 'div2airport': str,'div2tailnum':str},usecols=columns_list,iterator=True,chunksize=500)

for chunk in df_itr:
 df = pd.DataFrame(data=chunk, index=None)
 df1=df.fillna(0)
 df1.columns = map(str.lower, df1.columns)
 #df1_bytes = df1.to_csv(index = False)
 df1_bytes = df1.to_json(orient='records')
 producer.send(topic, df1_bytes)
 time.sleep(1)
