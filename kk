processed_data = (data1 
    | 'PrepareData' >> beam.ParDo(PrepareData()).with_output_types(mutation)
    | 'Reshuffle' >> beam.Reshuffle()  # Add reshuffle for better parallelism
)


--max_num_workers=50 \
--autoscaling_algorithm=THROUGHPUT_BASED \
--disk_size_gb=100 \
--worker_machine_type=n1-standard-4

max_number_mutations=2000,
        max_batch_size_bytes=5*1024*1024,


--experiments=use_runner_v2 \
--dataflow_service_options=worker_region=us-east4 \
--dataflow_service_options=enable_google_cloud_profiler \
--experiments=enable_heavyweight_hooks \
--job_endpoint_provider=dataflow \
--experiments=enable_streaming_engine \
--experiments=streaming_mode_execution_timeout_mins=240
