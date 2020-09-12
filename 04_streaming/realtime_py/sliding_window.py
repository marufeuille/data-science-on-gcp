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


def movingAverageOf(p, project, event, speed_up_factor):
  averagingInterval = 3600 / speed_up_factor
  averagingFrequency = averagingInterval / 2
  topic = "projects/{}/topics/{}".format(project, event)
  eventType = FieldNumberLookup.create(event)

  flights = (
    p 
    | "{}:read".format(event) >> beam.io.ReadFromPubSub(topic=topic)
    | "{}:window".format(event) >> beam.WindowInto(window.SlidingWindows(averagingInterval, averagingFrequency))
    | "{}:split".format(event) >> beam.Map(lambda elm: Flight(elm.decode("utf-8").split(","), eventType))
  )

  stats = {}

  stats["delay"] = (
    flights
    | "{}:delay".format(event) >> beam.Map(lambda elm: (elm.airport, elm.delay))
    | "{}:delay_mean".format(event) >> beam.combiners.Mean.PerKey()
  )

  stats["timestamp"] = (
    flights
    | "{}:timestamp".format(event) >> beam.Map(lambda elm: (elm.airport, elm.timestamp))
    | "{}:timestamp_max".format(event) >> beam.CombineGlobally(lambda elements: max(elements or [None])).without_defaults()
  )

  stats["num_flights"] = (
    flights
    | "{}:num_flights".format(event) >> beam.Map(lambda elm: (elm.airport, 1))
    | "{}:sum_num_flights".format(event) >> beam.combiners.Count.PerKey()
  )

  return stats


def run(project, speed_up_factor):
  """Build and run the pipeline."""

  argv = [
      '--project={0}'.format(project),
      '--job_name=ch04slidingwindow-py',
      '--save_main_session',
      '--runner=DirectRunner'
  ]

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(StandardOptions).streaming = True

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
      | 'Merge' >> beam.CoGroupByKey()
      | beam.Map(print)
    )

if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-s','--speed_up_factor', help='Bucket for staging', default=60)
   args = vars(parser.parse_args())
  
   run(project=args['project'], speed_up_factor=args['speed_up_factor'])