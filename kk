import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import apache_beam.io.gcp.spanner as sp
import json
import argparse
import logging
from typing import NamedTuple, Iterator, Optional
from datetime import datetime

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
    window_timestamp: Optional[str]
    insertion_timestamp: Optional[str]
beam.coders.registry.register_coder(ConvertToSpannerMutation, beam.coders.RowCoder)

class DecodeAvroRecords(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam) -> Iterator[dict]:
        import avro.io as avro_io
        import avro.schema
        from io import BytesIO
        import time

        def reformat_input_msg_schema(msg):
            fmt_msg = {}
            fmt_msg['timestamp'] = msg['timestamp']
            fmt_msg['host'] = msg['host']
            fmt_msg['src'] = msg['src']
            fmt_msg['ingressTimestamp'] = msg['_event_ingress_ts']
            fmt_msg['origins'] = [msg['_event_origin']]
            tags_len = len(msg['_event_tags'])
            if tags_len > 0:
                if msg['_event_tags'][0] != "" and msg['_event_tags'][0] is not None:
                    fmt_msg['tags'] = msg['_event_tags']
                else:
                    fmt_msg['tags'] = ['Dummy']
            else:
                fmt_msg['tags'] = ['Dummy']
            fmt_msg['route'] = 3
            fmt_msg['fetchTimestamp'] = int(time.time() * 1000)
            fmt_msg['rawdata'] = msg['rawdata']
            
            return json.loads(fmt_msg['rawdata'])

        message: dict = {}
        raw_schema = """{"namespace": "com.vz.vznet",
                        "type": "record",
                        "name": "VznetDefault",
                        "doc": "Default schema for events in transit",
                        "fields": [
                        {"name": "timestamp", "type": "long"},
                        {"name": "host", "type": "string"},
                        {"name": "src",  "type": "string" },
                        {"name": "_event_ingress_ts", "type": "long"},
                        {"name": "_event_origin", "type": "string"},
                        {"name": "_event_tags", "type": {"type": "array", "items": "string"}},
                        {"name": "_event_route", "type": "string"},
                        {"name": "_event_metrics", "type": ["null", "bytes"], "default": null},
                        {"name": "rawdata", "type": "bytes"}
                        ]}"""
        try:
            if isinstance(raw_schema, dict):
                raw_schema = json.dumps(raw_schema)
            schema = avro.schema.parse(raw_schema)
            avro_reader = avro_io.DatumReader(schema)
            logging.info(f"Processing message at pipeline timestamp: {timestamp.to_utc_datetime()}")
            if isinstance(element, bytes):
                avro_data = element
            else:
                avro_data = element.data if hasattr(element, 'data') else element
                
            if not isinstance(avro_data, bytes):
                logging.error(f"Expected bytes, got {type(avro_data)}")
                return
                
            avro_message = avro_io.BinaryDecoder(BytesIO(avro_data))
            message = avro_reader.read(avro_message)
            
            if schema.name == 'VznetDefault':
                message['rawdata'] = message['rawdata'].decode("utf-8")
                message['_event_metrics'] = message['_event_metrics'].decode("utf-8") if message['_event_metrics'] is not None else None
                
            yield reformat_input_msg_schema(message)
            
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            logging.error(f"Element type: {type(element)}")
            if hasattr(element, 'data'):
                logging.error(f"Element.data type: {type(element.data)}")
            return

class PrepareSpannerData(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam) -> Iterator[ConvertToSpannerMutation]:
        try:
            window_end = window.end.to_utc_datetime().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            yield self.transform_to_mutation(element, window_end, current_time)
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
            return

    def format_date(self, value):
        if value is None:
            return None
        try:
            if isinstance(value, str):
                dt = datetime.strptime(value.split('.')[0], '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            return value
        except Exception as e:
            logging.error(f"Date formatting error: {str(e)}")
            return None
    
    def safe_str(self, value):
        if value is None:
            return None
        try:
            return str(value)
        except Exception:
            return None

    def transform_to_mutation(self, element: dict, window_end: str, current_time: str):
        if not isinstance(element, dict):
            logging.error(f"Expected dictionary, got {type(element)}")
            return None
            
        if 'rawdata' in element:
            try:
                if isinstance(element['rawdata'], str):
                    element = json.loads(element['rawdata'])
                elif isinstance(element['rawdata'], dict):
                    element = element['rawdata']
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding rawdata: {str(e)}")
                return None
            
        return ConvertToSpannerMutation(
            telephone_number=element.get('telephone_number'),
            trouble_report_number=element.get('trouble_report_num'),
            address_id=element.get('address_id'),
            chronic_flag=element.get('chronic_flag'),
            chronic_total=element.get('chronic_total'),
            circuit_id=element.get('line_id_trimmed'),
            circuit_type=element.get('port_associated_service'),
            reported_ts=self.format_date(element.get('date_opened')),
            data_circuit_id=element.get('data_circuit_id'),
            video_circuit_id=self.safe_str(element.get('video_circuit_id')),
            window_timestamp=window_end,
            insertion_timestamp=current_time
        )

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
        data = (p 
            | "Read From Pubsub" >> ReadFromPubSub(
                subscription=f"{known_args.pubsub_subscription_name}",
                with_attributes=True
            )
            | "Window Into Fixed Intervals" >> beam.WindowInto(
                beam.window.FixedWindows(2 * 60),
                trigger=beam.trigger.AfterWatermark(),
                allowed_lateness=beam.window.Duration(seconds=300),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
            )
            | "Decode Avro" >> beam.ParDo(DecodeAvroRecords())
            | 'Prepare Spanner Data' >> beam.ParDo(PrepareSpannerData())
        )

        _ = (data
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
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--runner', default="DataflowRunner", help='Pipeline runner')
    parser.add_argument('--region', default="us-east4", help='GCP project region')
    parser.add_argument('--staging_location', required=True, help='Staging GCS bucket path')
    parser.add_argument('--temp_location', required=True, help='Temp GCS bucket path')
    parser.add_argument('--template_location', required=True, help='GCS bucket path to save template')
    parser.add_argument('--pubsub_subscription_name', required=True, help='Input Subscription')
    parser.add_argument('--sdk_container_image', default='us-east4-docker.pkg.dev/vz-it-np-gudv-dev-vzntdo-0/vznet/wireline:1.0.0', help='sdk_container_image location')
    parser.add_argument('--spanner_dataset', required=True, help='spanner dataset')
    parser.add_argument('--spanner_instance', required=True, help='spanner instance')
    parser.add_argument('--spanner_project', required=True, help='spanner project')
    parser.add_argument('--spanner_table', required=True, help='spanner table')
    parser.add_argument('--expansion_service', default='localhost:50768', help='Expansion service host:port')
    return parser.parse_known_args()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, beam_args = parse_arguments()
    run(known_args, beam_args)
    print(f'Created template at {known_args.template_location}.')
