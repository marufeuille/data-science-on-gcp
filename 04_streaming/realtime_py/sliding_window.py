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

  def __lt__(self, others):
    return self.name < others.name

  def __gt__(self, others):
    return self.name > others.name

  def __eq__(self, others):
    return self.name == others.name

class AirportStats():
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


def stats(elm):
  print(elm)
  airport = elm[0]
  values_dic = elm[1]
  arrDelay = values_dic["arr_delay"][0] if len(values_dic["arr_delay"]) > 0 else -999
  depDelay = values_dic["dep_delay"][0] if len(values_dic["dep_delay"]) > 0 else -999
  arrTs = values_dic["arr_timestamp"][0] if len(values_dic["arr_timestamp"]) > 0 else ""
  depTs = values_dic["dep_timestamp"][0] if len(values_dic["dep_timestamp"]) > 0 else ""
  arrNumflights = values_dic["arr_num_flights"][0] if len(values_dic["arr_num_flights"]) > 0 else 0
  depNumflights = values_dic["dep_num_flights"][0] if len(values_dic["dep_num_flights"]) > 0 else 0
  print(arrTs, depTs)
  timestamp = arrTs if arrTs > depTs else depTs
  num_flights = int(arrNumflights) + int(depNumflights)
  return AirportStats(airport, arrDelay, depDelay, timestamp, num_flights)


def movingAverageOf(p, project, event, speed_up_factor):
  averagingInterval = 3600 / speed_up_factor
  averagingFrequency = averagingInterval / 2
  topic = "projects/{}/topics/{}".format(project, event)
  eventType = FieldNumberLookup.create(event)

  flights = (
    p 
    | "{}:read".format(event) >> beam.io.ReadFromPubSub(topic=topic)
    | "{}:window".format(event) >> beam.WindowInto(window.SlidingWindows(averagingInterval, averagingFrequency))
    | "{}:parse".format(event) >> beam.Map(lambda elm: Flight(elm.decode("utf-8").split(","), eventType))
  )

  stats = {}

  stats["delay"] = (
    flights
    | "{}:airport_delay".format(event) >> beam.Map(lambda elm: (elm.airport, elm.delay))
    | "{}:avgdelay".format(event) >> beam.combiners.Mean.PerKey()
  )

  stats["timestamp"] = (
    flights
    | "{}:timestamps".format(event) >> beam.Map(lambda elm: (elm.airport, elm.timestamp))
    | "{}:lastTimeStamp".format(event) >> beam.CombineGlobally(lambda elem: max(elem or [None])).without_defaults()
  )

  stats["num_flights"] = (
    flights
    | "{}:numflights".format(event) >> beam.Map(lambda elm: (elm.airport, 1))
    | "{}:total".format(event) >> beam.combiners.Count.PerKey()
  )

  return stats

def toBQRow(stats):
  bq_row = {}
  bq_row["timestamp"] = stats.timestamp
  bq_row["airport"] = stats.airport.name
  bq_row["latitude"] = stats.airport.latitude
  bq_row["longitude"] = stats.airport.longitude

  if stats.dep_delay > -998:
    bq_row["dep_delay"] = stats.dep_delay
  if stats.arr_delay > -998:
    bq_row["arr_delay"] = stats.arr_delay

  bq_row["num_flights"] = stats.num_flights

  return bq_row



def run(project, speed_up_factor, dataset, output_table):
  """Build and run the pipeline."""

  argv = [
      '--project={0}'.format(project),
      '--job_name=ch04slidingwindow-py',
      '--save_main_session',
      '--runner=DirectRunner'
  ]

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(StandardOptions).streaming = True

  schema = "airport:string,latitude:float,longitude:float,timestamp:timestamp,dep_delay:float,arr_delay:float,num_flights:integer"

  with beam.Pipeline(options=pipeline_options) as p:
    arr = movingAverageOf(p, project, "arrived", speed_up_factor)
    dep = movingAverageOf(p, project, "departed", speed_up_factor)

    arr_delay = arr["delay"]
    arr_timestamp = arr["timestamp"]
    arr_num_flights = arr["num_flights"]

    dep_delay = dep["delay"]
    dep_timestamp = dep["timestamp"]
    dep_num_flights = dep["num_flights"]

    (
      (
        {
          'arr_delay': arr_delay,
          'arr_timestamp': arr_timestamp,
          'arr_num_flights': arr_num_flights,
          'dep_delay': dep_delay,
          'dep_timestamp': dep_timestamp,
          'dep_num_flights': dep_num_flights
        }
      )
      | 'airport:cogroup' >> beam.CoGroupByKey()
      | 'airport:stats' >> beam.Map(stats)
      | 'airport:to_BQrow' >> beam.Map(toBQRow)
      | beam.io.gcp.bigquery.WriteToBigQuery(
                table="{}:{}.{}".format(project, dataset, output_table),
                schema=schema
      )
    )

if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-s','--speed_up_factor', help='Bucket for staging', default=60)
   parser.add_argument('-o','--output_table', help='BigQuery Table where your data is stored after processing.', default='airport_stats')
   parser.add_argument('-d','--dataset', help='Bucket for staging', default='flights')
   args = vars(parser.parse_args())
  
   run(project=args['project'], speed_up_factor=args['speed_up_factor'], dataset=args['dataset'], output_table=args['output_table'])