import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterProcessingTime, Repeatedly
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import time
import random

# First, modify your pipeline options to enable streaming
options = {
    'project': known_args.project,
    'runner': known_args.runner,
    'region': known_args.region,
    'staging_location': known_args.staging_location,
    'temp_location': known_args.temp_location,
    'template_location': known_args.template_location,
    'setup_file': known_args.setup_file,
    'save_main_session': True,
    'streaming': True,  # Enable streaming mode
    'max_num_workers': 50,
    'autoscaling_algorithm': 'THROUGHPUT_BASED',
    'experiments': [
        'enable_streaming_engine',
        'streaming_mode_execution_timeout_mins=180'  # 3 hours runtime
    ]
}

pipeline_options = PipelineOptions.from_dictionary(options)
pipeline_options.view_as(StandardOptions).streaming = True

# Add a rate limiting DoFn
class RateLimitFn(beam.DoFn):
    """Rate-limits the processing of elements."""
    
    def __init__(self, elements_per_minute=10000):
        self.elements_per_minute = elements_per_minute
        self.sleep_time = 60.0 / elements_per_minute  # Time to sleep between elements
        self.last_element_time = 0
        
    def process(self, element, *args, **kwargs):
        current_time = time.time()
        if self.last_element_time > 0:
            time_since_last = current_time - self.last_element_time
            if time_since_last < self.sleep_time:
                time.sleep(self.sleep_time - time_since_last)
        
        self.last_element_time = time.time()
        yield element

# Add a batching function to control batch size
class BatchElements(beam.DoFn):
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.batch = []
        
    def process(self, element):
        self.batch.append(element)
        if len(self.batch) >= self.batch_size:
            yield self.batch
            self.batch = []
            
    def finish_bundle(self):
        if self.batch:
            yield beam.pvalue.TaggedOutput('batch', self.batch)

# Modify your pipeline
with beam.Pipeline(options=pipeline_options) as p:
    # Read from BigQuery (same as before)
    data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
        query=template_options.query,
        use_standard_sql=True,
        bigquery_job_labels={'parallelism': '50'}
    ))
    
    # Process the data
    processed_data = (data 
        | "FusionBreak1" >> beam.Reshuffle()
        | 'PrepareData' >> beam.ParDo(PrepareData()).with_output_types(mutation)
        | "Window" >> beam.WindowInto(
            FixedWindows(60),  # 1-minute windows
            trigger=AfterWatermark(early=AfterProcessingTime(10)),
            accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
        )
        | "BatchElements" >> beam.ParDo(BatchElements(batch_size=1000))
        | "FusionBreak2" >> beam.Reshuffle()
        | "RateLimit" >> beam.ParDo(RateLimitFn(elements_per_minute=10000))
    )
    
    # Write to Spanner with controlled rate
    _ = (processed_data | "WriteToSpanner" >> 
        sp.SpannerInsertOrUpdate(
            project_id=known_args.spanner_project,
            instance_id=known_args.spanner_instance,
            database_id=known_args.spanner_dataset,
            table=known_args.spanner_table,
            expansion_service=known_args.expansion_service,
            max_number_mutations=2000,
            max_batch_size_bytes=5*1024*1024,  # 5MB
        )
    )
