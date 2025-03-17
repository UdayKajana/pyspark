gcloud monitoring metrics list "spanner.googleapis.com/instance/cpu/utilization" \
  --project=PROJECT_ID \
  --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID AND resource.labels.database_id=DATABASE_ID" \
  --start-time="-1h" \
  --end-time="now"

gcloud monitoring metrics list "spanner.googleapis.com/instance/cpu/utilization" \
  --project=PROJECT_ID \
  --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
  --start-time="-1h" \
  --end-time="now" \
  --format="table(timeSeries.pointData.timeInterval.endTime, timeSeries.pointData.values.number.value)"


gcloud monitoring metrics list "spanner.googleapis.com/instance/cpu/utilization" \
  --project=PROJECT_ID \
  --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
  --start-time="-1h" \
  --end-time="now"
