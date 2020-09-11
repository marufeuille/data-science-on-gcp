from __future__ import absolute_import

import argparse
import logging
from os import pipe

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
#from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class AirPort():
  def __init__(self, fields, e):
    self.name = fields[e.FN_airport]
    self.latitude = float(fields[e.FN_lat])
    self.longitude = float(fields[e.FN_lon])

class AirPortStats():
  def __init__(self, airport, arr_delay, dep_delay, timestamp, num_flights):
    self.airport = airport
    self.arr_delay = arr_delay
    self.dep_delay = dep_delay
    self.timestamp = timestamp
    self.num_flights = num_flights

class Flight():
  def __init__(self, fields, e):
    self.airport = AirPort(fields, e)
    self.delay = float(fields[e.FN_delay])
    self.timestamp = fields[e.FN_timestamp]

class FieldNumberLookup():
  def __init__(self, e, fN_airport, fN_lat, fN_lon, fN_delay, fN_timestamp):
    self.eventName = e
    self.FN_airport = fN_airport - 1
    self.FN_lat = fN_lat - 1
    self.FN_lon = fN_lon - 1
    self.FN_delay = fN_delay - 1
    self.FN_timestamp = fN_timestamp - 1

  @staticmethod
  def create(event):
    if (event == "arrived"): 
      return FieldNumberLookup(event, 13, 31, 32, 23, 22)
    else:
      return FieldNumberLookup(event, 9, 28, 29, 16, 15)
		

class SplitFn(beam.DoFn):
  def __init__(self, eventType):
    self.eventType = eventType

  def process(self, element):
    return [Flight(element.decode("utf-8").split(","), self.eventType)]

def create_table(fields):
  header = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME'.split(',')

  featdict = {}
  for name, value in zip(header, fields):
    if value == '':
      continue
    featdict[name] = value
  return featdict


def movingAverageOf(p, project, event):
  averagingInterval = 3600 / 60
  averagingFrequency = averagingInterval / 2
  topic = "projects/{}/topics/{}".format(project, event)
  eventType = FieldNumberLookup.create(event)

  flights = (
    p 
    | beam.io.ReadFromPubSub(topic=topic)
    | beam.WindowInto(window.SlidingWindows(averagingInterval, averagingFrequency))
    | beam.ParDo(SplitFn(eventType))
  )

  delay = (
    flights
    | beam.Map(lambda elm: (elm.airport, elm.delay))
    | beam.combiners.Mean.PerKey()
    | beam.Map(lambda elm: print(elm[0].name, elm[1]))
  )


def run(project, output_table, bucket, dataset):
  """Build and run the pipeline."""

  argv = [
      '--project={0}'.format(project),
      '--job_name=ch04streaming-py',
      '--save_main_session',
      '--staging_location=gs://{0}/flights/streaming/staging'.format(bucket),
      '--temp_location=gs://{0}/flights/streaming/temp/'.format(bucket),
      '--max_num_workers=1',
      '--autoscaling_algorithm=THROUGHPUT_BASED',
      '--runner=DirectRunner'
  ]

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(StandardOptions).streaming = True

  output_table = "{}:{}.{}".format(project, dataset, output_table)

  with beam.Pipeline(options=pipeline_options) as p:
    movingAverageOf(p, project, "departed")


if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-o','--output_table', help='BigQuery Table where your data is stored after processing.', default='raw_output')
   parser.add_argument('-b','--bucket', help='Bucket for staging', default='bigdata_demo')
   parser.add_argument('-d','--dataset', help='Bucket for staging', default='flights')
   args = vars(parser.parse_args())
  
   run(project=args['project'], output_table=args['output_table'], bucket=args['bucket'], dataset=args['dataset'])