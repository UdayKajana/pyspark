gcloud spanner databases execute-sql wireline_churn \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="SELECT * FROM INFORMATION_SCHEMA.DATABASES"


gcloud spanner databases execute-sql wireline_churn \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="SELECT * FROM projects/vz-it-np-jpuv-dev-anpdo-0/instances/anp-spanner-instance/databases/network_wls_model/tables/test_table_1"


gcloud spanner databases execute-sql wireline_churn \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="WITH local_data AS (SELECT * FROM test_table_2)
        SELECT * FROM local_data 
        WHERE id IN (SELECT id FROM network_wls_model.test_table_1)"
