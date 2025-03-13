import argparse
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

logging.getLogger().setLevel(logging.INFO)
known_args, beam_args = parse_arguments()

mutation = getMutation(known_args.process)
beam.coders.registry.register_coder(mutation, beam.coders.RowCoder)

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
        use_standard_sql=True
    ))
    processed_data = (data | 'PrepareData' >> 
        beam.ParDo(PrepareData())
        .with_output_types(mutation))
    # Write to Spanner
    _ = (processed_data | "WriteToSpanner" >> 
        sp.SpannerInsertOrUpdate(
            project_id=known_args.spanner_project,
            instance_id=known_args.spanner_instance,
            database_id=known_args.spanner_dataset,
            table=known_args.spanner_table,
            expansion_service=known_args.expansion_service
        )
    )



I am using Kajana Uday
 Mar 4th at 11:39
java -jar /apps/opt/application/expansion_service_jars/beam-sdks-java-extensions-sql-expansion-service-2.61.0.jar 50768 as expansion service.

this code is failing to load 100cr date, please let me know does this pipeline to be optimized anymore?
