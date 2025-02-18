class ListOfConvertToSpannerMutationCoder(beam.coders.Coder):
    def __init__(self, coder):
        self._coder = coder

    def encode(self, value):
        return b''.join([self._coder.encode(v) for v in value])

    def decode(self, value):
        # Assuming the values are concatenated without length-prefix
        elements = []
        while value:
            element, value = self._coder.decode_with_remainder(value)
            elements.append(element)
        return elements

    def is_deterministic(self):
        return self._coder.is_deterministic()

class PrepareSpannerData(beam.DoFn):
    def process(self, element) -> Iterator[ConvertToSpannerMutation]:
        try:
            yield self.transform_to_mutation(element)
        except Exception as e:
            logging.error(f"Error processing element: {element}")
            logging.error(f"Error details: {str(e)}")
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
        data = (p 
            | "Read From Pubsub" >> ReadFromPubSub(
                subscription=f"{known_args.pubsub_subscription_name}")
            | "Decode Avro" >> beam.ParDo(DecodeAvroRecords())
        )
        windowed_data = (data 
            | "Windowing" >> beam.WindowInto(beam.window.FixedWindows(2 * 60)))
        prepared_data = (windowed_data
            | 'Prepare Spanner Data' >> beam.ParDo(PrepareSpannerData())
        )
        batched_data = (prepared_data
            | "Batch Elements" >> BatchElements(min_batch_size=100, max_batch_size=7000)
        )
        batched_data = batched_data | "Set Coder" >> beam.Map(lambda x: x).with_output_types(List[ConvertToSpannerMutation])
        
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
