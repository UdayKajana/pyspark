import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import apache_beam.io.gcp.spanner as sp

# Rate limiting DoFn that controls the flow of elements
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

# Your existing pipeline code with the rate limiting added
options = {
    'project': known_args.project,
    'runner': known_args.runner,
    'region': known_args.region,
    'staging_location': known_args.staging_location,
    'temp_location': known_args.temp_location,
    'template_location': known_args.template_location,
    'setup_file': known_args.setup_file,
    'save_main_session': True,
    'streaming': False  # Keep as batch pipeline
}

pipeline_options = PipelineOptions.from_dictionary(options)
template_options = pipeline_options.view_as(TemplateOptions)

with beam.Pipeline(options=pipeline_options) as p:
    # Read from BigQuery - unchanged
    data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
        query=template_options.query,
        use_standard_sql=True,
        bigquery_job_labels={'parallelism': '50'}
    ))
    
    # Prepare data - unchanged
    data1 = (data | "FusionBreak" >> beam.Reshuffle())
    processed_data = (data1 | 'PrepareData' >> 
        beam.ParDo(PrepareData())
        .with_output_types(mutation))
    
    # Add rate limiting before Spanner write
    rate_limited_data = (processed_data 
        | "RateLimitBeforeWrite" >> beam.ParDo(RateLimitFn(elements_per_minute=100000))
        | "BatchForSpanner" >> beam.BatchElements(
            min_batch_size=500,
            max_batch_size=2000
        )
    )
    
    # Write to Spanner with smaller batch size
    _ = (rate_limited_data | "WriteToSpanner" >> 
        sp.SpannerInsertOrUpdate(
            project_id=known_args.spanner_project,
            instance_id=known_args.spanner_instance,
            database_id=known_args.spanner_dataset,
            table=known_args.spanner_table,
            expansion_service=known_args.expansion_service,
            max_number_mutations=2000,
            max_batch_size_bytes=4*1024*1024  # 4MB batch size
        )
    )
