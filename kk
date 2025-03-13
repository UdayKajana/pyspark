data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(
    query=template_options.query,
    use_standard_sql=True,
    # Add these parameters:
    use_json_exports=True,  # Often faster for large datasets
    # Set a reasonable number of parallel reads based on your data size
    # For 100 crore (1 billion) records, try:
    bigquery_job_labels={'parallelism': '100'}
))



data = (p | "ReadFromBigQuery" >> ReadFromBigQuery(...))

# Add this line to prevent fusion with downstream operations
data = (data | "FusionBreak" >> beam.Reshuffle())

processed_data = (data | 'PrepareData' >> beam.ParDo(PrepareData())...)


# Batch writes to Spanner (improves throughput)
_ = (processed_data 
     | "BatchElements" >> beam.BatchElements(min_batch_size=100, max_batch_size=1000)
     | "WriteToSpanner" >> sp.SpannerInsertOrUpdate(...))
