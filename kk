gcloud spanner databases execute-sql wireline_churn \
  --instance=anp-spanner-instance \
  --project=vz-it-np-jpuv-dev-anpdo-0 \
  --sql="SELECT t1.id as t1_id, t1.name as t1_name, t2.id as t2_id, t2.name as t2_name 
        FROM test_table_2 as t1 
        INNER JOIN network_wls_model.test_table_1 as t2 ON t1.id = t2.id 
        ORDER BY t1.id"
