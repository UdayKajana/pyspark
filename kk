import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import apache_beam.io.gcp.spanner as sp
import json
import argparse
import logging
from typing import NamedTuple, Iterator, Optional
from datetime import datetime
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.transforms.window import FixedWindows, TimestampCombiner

class TemplateOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--params', type=str)

class ConvertToSpannerMutation(NamedTuple):
    telephone_number: Optional[str]
    trouble_report_number: Optional[str]
    address_id: Optional[str]
    chronic_flag: Optional[str]
    chronic_total: Optional[str]
    circuit_id: Optional[str]
    circuit_type: Optional[str]
    reported_ts: Optional[str]
    data_circuit_id: Optional[str]
    video_circuit_id: Optional[str]

beam.coders.registry.register_coder(ConvertToSpannerMutation, beam.coders.RowCoder)

class DecodeAvroRecords(beam.DoFn):
    # [Previous DecodeAvroRecords implementation remains the same]
    # Your existing DecodeAvroRecords class implementation here...

class PrepareSpannerData(beam.DoFn):
    # [Previous PrepareSpannerData implementation remains the same]
    # Your existing PrepareSpannerData class implementation here...

class BatchAndProcessFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # element will be a list of mutations from the GroupByKey
        try:
            for item in element:
                yield item
        except Exception as e:
            logging.error(f"Error in BatchAndProcessFn: {str(e)}")
            return

def run(known_args, beam_args):
    options = {
        'project': known_args.project,
        'runner': known_args.runner,
        'region': known_args.region,
        'staging_location': known_args.staging_location,
        'temp_location': known_args.temp_location,
        'template_location': known_args.template_location,
        'save_main_session': True,
        'streaming': True,
        'sdk_container_image': known_args.sdk_container_image,
        'sdk_location': 'container'
    }

    pipeline_options = PipelineOptions.from_dictionary(options)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Read and decode data
        data = (p 
            | "Read From Pubsub" >> ReadFromPubSub(
                subscription=known_args.pubsub_subscription_name)
            | "Decode Avro" >> beam.ParDo(DecodeAvroRecords())
        )

        # Process and batch data
        mutations = (data
            | 'Prepare Spanner Data' >> beam.ParDo(PrepareSpannerData())
            # Add timestamp to enable windowing
            | 'Add Timestamps' >> beam.Map(
                lambda x: beam.window.TimestampedValue(x, beam.utils.timestamp.Timestamp.now())
            )
            # Window into 2-minute fixed windows
            | 'Window' >> beam.WindowInto(
                FixedWindows(120),  # 2 minutes in seconds
                trigger=AfterWatermark(early=None, late=None),
                accumulation_mode=AccumulationMode.DISCARDING,
                timestamp_combiner=TimestampCombiner.END_OF_WINDOW
            )
            # Group elements in the window using a dummy key
            | 'Add Key' >> beam.Map(lambda x: ('key', x))
            | 'Group By Window' >> beam.GroupByKey()
            | 'Remove Key' >> beam.MapTuple(lambda _, value: value)
            # Process batched elements
            | 'Process Batched Elements' >> beam.ParDo(BatchAndProcessFn())
        )

        # Write to Spanner
        _ = (mutations
            | "Write To Spanner" >> sp.SpannerInsertOrUpdate(
                instance_id=known_args.spanner_instance,
                database_id=known_args.spanner_dataset,
                project_id=known_args.spanner_project,
                table=known_args.spanner_table,
                expansion_service=known_args.expansion_service,
                max_number_mutations=7000,
                high_priority=True
            )
        )

def parse_arguments():
    parser = argparse.ArgumentParser()
    # [Previous argument parsing code remains the same]
    # Your existing argument parsing code here...
    return parser.parse_known_args()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, beam_args = parse_arguments()
    run(known_args, beam_args)
    print(f'Created template at {known_args.template_location}.')
