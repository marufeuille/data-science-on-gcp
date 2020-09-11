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


def create_table(fields):
  header = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE,DEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET,ARR_AIRPORT_LAT,ARR_AIRPORT_LON,ARR_AIRPORT_TZOFFSET,EVENT,NOTIFY_TIME'.split(',')

  featdict = {}
  for name, value in zip(header, fields):
    if value == '':
      continue
    featdict[name] = value
  return featdict



def run(project, input_topic, output_table, bucket, dataset):
  """Build and run the pipeline."""

  argv = [
      '--project={0}'.format(project),
      '--job_name=ch04streaming-py',
      '--save_main_session',
      '--staging_location=gs://{0}/flights/streaming/staging'.format(bucket),
      '--temp_location=gs://{0}/flights/streaming/temp/'.format(bucket),
      '--max_num_workers=1',
      '--autoscaling_algorithm=THROUGHPUT_BASED',
      '--runner=DataflowRunner'
  ]

  schema = 'FL_DATE:date,UNIQUE_CARRIER:string,AIRLINE_ID:string,CARRIER:string,FL_NUM:string,ORIGIN_AIRPORT_ID:string,ORIGIN_AIRPORT_SEQ_ID:integer,ORIGIN_CITY_MARKET_ID:string,ORIGIN:string,DEST_AIRPORT_ID:string,DEST_AIRPORT_SEQ_ID:integer,DEST_CITY_MARKET_ID:string,DEST:string,CRS_DEP_TIME:timestamp,DEP_TIME:timestamp,DEP_DELAY:float,TAXI_OUT:float,WHEELS_OFF:timestamp,WHEELS_ON:timestamp,TAXI_IN:float,CRS_ARR_TIME:timestamp,ARR_TIME:timestamp,ARR_DELAY:float,CANCELLED:string,CANCELLATION_CODE:string,DIVERTED:string,DISTANCE:float,DEP_AIRPORT_LAT:float,DEP_AIRPORT_LON:float,DEP_AIRPORT_TZOFFSET:float,ARR_AIRPORT_LAT:float,ARR_AIRPORT_LON:float,ARR_AIRPORT_TZOFFSET:float,EVENT:string,NOTIFY_TIME:timestamp,EVENT_DATA:string'

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | beam.io.ReadFromPubSub(topic="projects/{}/topics/{}".format(project, input_topic))

    (lines
      | 'flights:toarr' >> beam.Map(lambda fields: fields.decode("utf-8").split(","))
      | 'flights:create_bq_row' >> beam.Map(lambda fields: create_table(fields)) 
      | beam.io.gcp.bigquery.WriteToBigQuery(
                table="{}:{}.{}".format(project, dataset, output_table),
                schema=schema
      )
    )


if __name__ == '__main__':
   logging.getLogger().setLevel(logging.INFO)
   parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
   parser.add_argument('-p','--project', help='Unique project ID', required=True)
   parser.add_argument('-i','--input_topics', help='PubSub Topics where your data is arrived', default="arrived")
   parser.add_argument('-o','--output_table', help='BigQuery Table where your data is stored after processing.', default='raw_output')
   parser.add_argument('-b','--bucket', help='Bucket for staging', default='bigdata_demo')
   parser.add_argument('-d','--dataset', help='Bucket for staging', default='flights')
   args = vars(parser.parse_args())
  
   run(project=args['project'], input_topic=args['input_topics'], output_table=args['output_table'], bucket=args['bucket'], dataset=args['dataset'])