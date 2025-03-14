gcloud spanner instances describe INSTANCE_ID --format="default(nodeCount,processingUnits,state,config)"

gcloud monitoring metrics list | grep spanner


gcloud monitoring metrics describe spanner.googleapis.com/instance/cpu/utilization


gcloud monitoring time-series list \
    "spanner.googleapis.com/instance/cpu/utilization" \
    --instance-id=INSTANCE_ID \
    --project=PROJECT_ID \
    --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
    --time-series-duration=P1D


gcloud monitoring time-series list \
    "spanner.googleapis.com/instance/cpu/utilization" \
    --project=PROJECT_ID \
    --filter="resource.type=spanner_instance AND resource.labels.instance_id=INSTANCE_ID" \
    --time-series-duration=PT1H \
    --format="csv(time.iso,value)"
