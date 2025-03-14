import argparse
import time
from mutations.Mutations import *
import logging
from typing import NamedTuple, Iterator, Optional
import apache_beam as beam
import apache_beam.io.gcp.spanner as sp
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from decimal import Decimal
from datetime import date, datetime

class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--query', type=str)
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Project ID')
    parser.add_argument('--runner', default="DataflowRunner", help='Pipeline runner')
    parser.add_argument('--region', default="us-east4", help='GCP project region')
    parser.add_argument('--staging_location', required=True, help='Staging GCS bucket path')
    parser.add_argument('--temp_location', required=True, help='Temp GCS bucket path')
    parser.add_argument('--template_location', required=True, help='GCS bucket path to save template')
    parser.add_argument('--spanner_dataset', required=True, help='spanner dataset')
    parser.add_argument('--spanner_instance', required=True, help='spanner instance')
    parser.add_argument('--spanner_project', required=True, help='spanner project')
    parser.add_argument('--spanner_table', required=True, help='spanner table')
    parser.add_argument('--process', required=True, help='process')
    parser.add_argument('--expansion_service', default='localhost:50768', help='Expansion service host:port')
    parser.add_argument('--setup_file', dest='setup_file', default='./setup.py', required=False, help='setup_file')
    return parser.parse_known_args()

logging.getLogger().setLevel(logging.ERROR)
known_args, beam_args = parse_arguments()

mutation = getMutation(known_args.process)
beam.coders.registry.register_coder(mutation, beam.coders.RowCoder)

class RateLimitFn(beam.DoFn):
    def __init__(self, elements_per_minute=100000):
        self.elements_per_minute = elements_per_minute
        self.elements_per_second = elements_per_minute / 60.0
        self.last_yield_time = None
        self.counter = 0
        
    def start_bundle(self):
        self.last_yield_time = time.time()
        self.counter = 0
    
    def process(self, element):
        current_time = time.time()
        self.counter += 1
        
        # Check if we need to throttle
        if self.counter >= self.elements_per_second:
            elapsed = current_time - self.last_yield_time
            if elapsed < 1.0:
                sleep_time = 1.0 - elapsed
                time.sleep(sleep_time)
            
            # Reset counter and timer
            self.counter = 0
            self.last_yield_time = time.time()
        
        yield element


class PrepareData(beam.DoFn):
    def process(self, element) -> Iterator[NQESSiteScores]:
        try:
            yield self.transform_to_mutation(element)
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
            raise
    def transform_to_mutation(self, row):
        return mutation.setMutation(row)

options = {
    'project': known_args.project,
    'runner': known_args.runner,
    'region': known_args.region,
    'staging_location': known_args.staging_location,
    'temp_location': known_args.temp_location,
    'template_location': known_args.template_location,
    'setup_file': known_args.setup_file,
    'save_main_session': True,
    'streaming': False
}

pipeline_options = PipelineOptions.from_dictionary(options)
template_options = pipeline_options.view_as(TemplateOptions)
with beam.Pipeline(options=pipeline_options) as p:
    # Read from BigQuery
    data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
        query=template_options.query,
        use_standard_sql=True,
        bigquery_job_labels={'parallelism': '50'}
    ))
    data1 = (data | "FusionBreak" >> beam.Reshuffle())
    processed_data = (data1 | 'PrepareData' >> 
        beam.ParDo(PrepareData())
        .with_output_types(mutation))
    # Write to Spanner

    rate_limited_data = (processed_data 
        | "RateLimitBeforeWrite" >> beam.ParDo(RateLimitFn(elements_per_minute=100000))
        | "BatchForSpanner" >> beam.BatchElements(
            min_batch_size=500,
            max_batch_size=2000
        )
    )
    _ = (rate_limited_data | "WriteToSpanner" >> 
        sp.SpannerInsertOrUpdate(
            project_id=known_args.spanner_project,
            instance_id=known_args.spanner_instance,
            database_id=known_args.spanner_dataset,
            table=known_args.spanner_table,
            expansion_service=known_args.expansion_service,
            max_number_mutations=5000,
            max_batch_size_bytes=1048576,
            max_number_rows = 5000,
            
        )
    )



with this code, I am getting the following error: 
