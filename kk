import json
import avro.io as avro_io
import avro.schema
from io import BytesIO
from avro.datafile import DataFileReader, DataFileWriter
import io
import logging
import argparse
from subprocess import getoutput
import random
import time

def pushToTopic(data_rows, topic_name):
    from concurrent import futures
    from google.cloud import pubsub_v1
    from google.cloud.pubsub_v1.types import (
        LimitExceededBehavior,
        PublisherOptions,
        PublishFlowControl,
    )

    project_id = "vz-it-np-gudv-dev-vzntdo-0"
    topic_id = topic_name

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    publish_futures = []

    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        message_id = publish_future.result()

    publish_future = publisher.publish(topic_path, data_rows)
    publish_future.add_done_callback(callback)
    publish_futures.append(publish_future)
    print(f"Published messages with flow control settings to {topic_path}.")

def EncodeAvro(element):
    def encode(data: dict) -> bytes:
        schema = """{"namespace": "com.vz.vznet",
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
                                                               ]
                                                               }"""
        if isinstance(schema, dict):
            schema = json.dumps(schema)
        if isinstance(data, str):
            data = json.loads(data)
        schema: str = avro.schema.parse(schema)
        val = data
        datum_writer: avro_io.DatumWriter = avro_io.DatumWriter(schema)
        bytes_writer: io.BytesIO = io.BytesIO()
        encoder: avro_io.BinaryEncoder = avro_io.BinaryEncoder(bytes_writer)
        datum_writer.write(val, encoder)
        raw_bytes: bytes = bytes_writer.getvalue()
        return raw_bytes

    raw_data = element['rawdata']
    final_dict = {}
    final_dict['timestamp'] = element['timestamp']
    final_dict['src'] = element['src']
    final_dict['host'] = element['host']
    final_dict['_event_ingress_ts'] = element['ingressTimestamp']
    final_dict['_event_origin'] = '|'.join(element['origins'])
    final_dict["rawdata"] = json.dumps(raw_data)
    final_dict['_event_tags'] = element['tags']
    final_dict['_event_route'] = str(element['route'])
    final_dict['_event_metrics'] = None
    final_dict["rawdata"] = bytes(json.dumps(raw_data), 'utf-8')
    return encode(final_dict)

# Base message template
message_template = {
    "src": "vz.pip.eclipse.stat.if_stats.proc.v0", 
    "timestamp": 1563152004139, 
    "host": "TEST-1234-XYZ",
    "ingressTimestamp": 1563153004139, 
    "fetchTimestamp": 1563153008139, 
    "origins": ["vmb,kafka,ENMV.PIP.IP"],
    "tags": [], 
    "route": 3,
    "rawdata": {
        "telephone_number": "2022-12-02 05:00:00.000000 UTC",
        "addres1s_id": "123",
        "chronic_flag": "LGF8pf4gkOZcABnNq9NMxNcE/U2Gc0B3trVK0hEzNp7v5Z7ONCJVcILwpbzVuqyOFxz2XIU/rjOerp6NEmjmnQ==",
        "chronic1_total": "BWLHiXyrs2l8/F7ldbmoRsIY7WQhFyX3euXshykBAALkRUlgQLvnnltzAp/ymwFAjc0Auk7LVFC4InQT+g5/1Q==",
        "line_id_trimmed": "20808267725.0",
        "port_as1sociated_service": "ZcXG3TS/LG7ymRvKTaf4C7lUgLMO3MVHIhhQ74DRZ9Y6opYN1hADfRhn4SA+eXs5kbweoYtXXeAVLyo8XD1ioQ==",
        "date_opened": "2019-08-28 04:00:00.000000 UTC",
        "data_circuit_id": "SFU",
        "video_circuit_id": "Residence"
    }
}

# Generate and push 100 distinct messages
for i in range(100):
    # Create a new message with unique trouble_report_num
    message = message_template.copy()
    message['rawdata'] = message_template['rawdata'].copy()
    message['rawdata']['trouble_report_num'] = f"TR{str(random.randint(100000, 999999))}"
    
    # Encode and push the message
    encoded_message = EncodeAvro(message)
    pushToTopic(encoded_message, "vz.common.vrepair.ticket.opened.norm.v0")
    print(f"Pushed message {i+1}/100 with trouble_report_num: {message['rawdata']['trouble_report_num']}")
    
    # Small delay to avoid potential rate limiting
    time.sleep(0.1)
