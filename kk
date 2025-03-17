gcloud monitoring time-series list \
  "spanner.googleapis.com/instance/cpu/utilization" \
  --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
  --project=PROJECT_ID \
  --interval=now-1h,now

gcloud monitoring metrics descriptors list \
  --filter="metric.type=spanner.googleapis.com/instance/cpu/utilization"


gcloud monitoring time-series list \
  "spanner.googleapis.com/instance/cpu/utilization" \
  --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
  --project=PROJECT_ID \
  --interval=now-1h,now \
  --format="table(timeSeriesData.pointData.timeInterval.endTime, timeSeriesData.pointData.values.doubleValue)"
