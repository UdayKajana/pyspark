import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import apache_beam.io.gcp.spanner as sp
import json
import argparse
import logging
from typing import NamedTuple, Iterator, Optional
from datetime import datetime
from apache_beam.transforms.util import BatchElements
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.typehints import schemas
from apache_beam.coders.proto2_coder import Proto2Coder

# Define the schema for cross-language compatibility
SCHEMA = beam_runner_api_pb2.Schema(
    id="spanner_mutation_schema",
    fields={
        "telephone_number": beam_runner_api_pb2.Schema.Field(
            name="telephone_number", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "trouble_report_number": beam_runner_api_pb2.Schema.Field(
            name="trouble_report_number", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "address_id": beam_runner_api_pb2.Schema.Field(
            name="address_id", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "chronic_flag": beam_runner_api_pb2.Schema.Field(
            name="chronic_flag", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "chronic_total": beam_runner_api_pb2.Schema.Field(
            name="chronic_total", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "circuit_id": beam_runner_api_pb2.Schema.Field(
            name="circuit_id", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "circuit_type": beam_runner_api_pb2.Schema.Field(
            name="circuit_type", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "reported_ts": beam_runner_api_pb2.Schema.Field(
            name="reported_ts", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "data_circuit_id": beam_runner_api_pb2.Schema.Field(
            name="data_circuit_id", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING)),
        "video_circuit_id": beam_runner_api_pb2.Schema.Field(
            name="video_circuit_id", type=beam_runner_api_pb2.Schema.FieldType(atomic_type=beam_runner_api_pb2.Schema.FieldType.STRING))
    }
)

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

# Register schema with beam
beam.transforms.core.register_coder(ConvertToSpannerMutation, beam.coders.RowCoder(SCHEMA))

class PortableSpannerMutation(beam.DoFn):
    def process(self, element: ConvertToSpannerMutation):
        # Convert to a format that's compatible with Java
        mutation_dict = element._asdict()
        # Ensure all values are strings or None
        mutation_dict = {k: str(v) if v is not None else None for k, v in mutation_dict.items()}
        yield mutation_dict

class DecodeAvroRecords(beam.DoFn):
    # ... [rest of the DecodeAvroRecords class remains the same]

class PrepareSpannerData(beam.DoFn):
    # ... [rest of the PrepareSpannerData class remains the same]

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
        'sdk_location': 'container',
        'experiments': [
            'use_portable_transforms=true',
            'use_runner_v2=true'
        ]
    }

    pipeline_options = PipelineOptions.from_dictionary(options)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Read and decode data
        data = (p 
            | "Read From Pubsub" >> ReadFromPubSub(
                subscription=f"{known_args.pubsub_subscription_name}")
            | "Decode Avro" >> beam.ParDo(DecodeAvroRecords())
        )

        # Window the data
        windowed_data = (data 
            | "Windowing" >> beam.WindowInto(beam.window.FixedWindows(2 * 60)))

        # Prepare the data
        mutation_data = (windowed_data
            | 'Prepare Spanner Data' >> beam.ParDo(PrepareSpannerData())
            | 'Convert to Portable Format' >> beam.ParDo(PortableSpannerMutation()))

        # Batch the data with explicit coder
        batched_data = (mutation_data
            | "Batch Elements" >> BatchElements(
                min_batch_size=100,
                max_batch_size=7000
            ).with_output_types(beam.typehints.List[dict]))

        # Write to Spanner
        _ = (batched_data
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

# ... [rest of the code remains the same]
