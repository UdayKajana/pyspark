gcloud spanner databases execute-sql wireline_churn \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="SELECT * FROM  test_table_2"

gcloud spanner databases execute-sql network_wls_model \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="SELECT * FROM  test_table_1"
